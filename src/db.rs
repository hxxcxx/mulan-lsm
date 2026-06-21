//! `Db`：编排 MemTable + WAL + SSTable + VersionSet 的 LSM 主类。
//!
//! M4 的 DB 是单线程串行模型（并发协调留到 M5）。写路径：`put`/`delete` → MemTable + WAL；
//! MemTable 攒满后 flush 成 L0 SSTable，写 `VersionEdit` 提交（manifest 落盘即生效），切换到新 WAL。
//! 读路径：MemTable → 所有 L0 SSTable（按从新到旧，命中即返回；遇 Delete 返回不存在）。

use crate::error::{MulanError, Result};
use crate::file_meta::{
    current_path, log_path, parse_log_name, parse_manifest_name, parse_sst_name, sst_path,
    FileMetaData, FileNumber, IdGenerator, CURRENT_NAME, TMP_SUFFIX,
};
use crate::internal_key::ValueType;
use crate::manifest::{write_current, VersionEdit};
use crate::memtable::{FlushResult, MemTable};
use crate::sstable::TableReader;
use crate::version::{VersionSet, NUM_LEVELS};
use crate::wal::{decode_entry, encode_entry, WalReader, WalWriter};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

/// DB 配置。M4 用小阈值便于测试触发 flush。
#[derive(Debug, Clone)]
pub struct Options {
    /// MemTable 攒到多少条触发 flush。LevelDB 默认 ~4MB，这里用条目数简化。
    pub memtable_flush_entries: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_flush_entries: 1000,
        }
    }
}

/// mulan-lsm 数据库主类。一个实例对应一个目录。
pub struct Db {
    dir: PathBuf,
    version_set: VersionSet,
    memtable: MemTable,
    wal: WalWriter,
    id_gen: IdGenerator,
    /// 当前正在写入的 WAL 编号。
    wal_number: FileNumber,
    options: Options,
}

impl Db {
    /// 打开（或创建）一个数据库。
    ///
    /// 空目录 → 新建库；存在 CURRENT → 走恢复路径（回放 manifest + WAL，清理孤儿文件）。
    pub fn open(dir: &Path, options: Options) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        if current_path(dir).exists() {
            return Self::recover_open(dir, options);
        }
        // 无 CURRENT：必须是空目录，否则视为残留状态，拒绝初始化以免覆盖。
        let is_empty = std::fs::read_dir(dir)?.next().is_none();
        if !is_empty {
            return Err(MulanError::InvalidArgument(format!(
                "directory {:?} is not empty and has no CURRENT; refusing to initialize",
                dir
            )));
        }
        Self::create_new(dir, options)
    }

    /// 恢复已有库：回放 manifest 重建 VersionSet → 回放当前 WAL 成 MemTable →
    /// 若 MemTable 非空则 flush 成 L0（写一条新 VersionEdit 提交）→ 打开下一个 WAL →
    /// 清理孤儿文件（不属于任何 Version 的 SSTable、非当前 WAL、旧 Manifest）。
    fn recover_open(dir: &Path, options: Options) -> Result<Self> {
        let mut version_set = VersionSet::recover(dir)?;
        let mut id_gen = IdGenerator::new(version_set.next_file_number);

        // 回放当前 WAL（编号 = version_set.log_number）成 MemTable。
        let mut memtable = MemTable::with_initial_sequence(version_set.last_sequence);
        let current_log = FileNumber(version_set.log_number);
        let current_log_path = log_path(dir, current_log);
        if current_log_path.exists() {
            let reader = WalReader::open(&current_log_path)?;
            for rec in reader.read_records()? {
                let entry = decode_entry(&rec)?;
                memtable.apply(entry.vtype, entry.seq, &entry.key, &entry.value);
            }
        }

        // 回放出的 MemTable 若非空，flush 成 L0 并提交。
        let wal_number;
        if memtable.num_entries() > 0 {
            let sst_number = id_gen.new_file_number();
            let path = sst_path(dir, sst_number);
            let FlushResult {
                smallest, largest, ..
            } = memtable.flush_to_sstable_with_bounds(&path)?;
            let file_size = std::fs::metadata(&path)?.len();
            let (smallest, largest) = match (smallest, largest) {
                (Some(s), Some(l)) => (s, l),
                _ => return Err(MulanError::Corrupted("recovered memtable empty".into())),
            };
            let meta = FileMetaData::new(sst_number, file_size, smallest, largest);
            let new_log_number = id_gen.new_file_number();
            let mut edit = VersionEdit::new();
            edit.add_file(0, meta)
                .set_log_number(new_log_number.0)
                .set_next_file_number(id_gen.next_number())
                .set_last_sequence(memtable.sequence());
            version_set.write_new_version(&edit)?;
            memtable = MemTable::with_initial_sequence(memtable.sequence());
            wal_number = new_log_number;
        } else {
            // MemTable 空：复用当前 WAL 继续追加（WalWriter 以 append 模式从文件尾续写）。
            wal_number = current_log;
        }

        let wal = WalWriter::create(&log_path(dir, wal_number))?;

        // 清理孤儿文件。
        remove_obsolete_files(dir, &version_set)?;

        Ok(Db {
            dir: dir.to_path_buf(),
            version_set,
            memtable,
            wal,
            id_gen,
            wal_number,
            options,
        })
    }

    /// 新建库：分配 manifest + 首个 WAL，写初始 VersionEdit，落 CURRENT。
    fn create_new(dir: &Path, options: Options) -> Result<Self> {
        let mut id_gen = IdGenerator::new(1);
        // 编号分配：1 = manifest，2 = 首个 WAL。
        let manifest_number = id_gen.new_file_number();
        let log_number = id_gen.new_file_number();
        let mut version_set = VersionSet::new_pending(dir, manifest_number)?;
        // 初始 edit 记录 comparator、当前 log、下一个可用编号、起始 seq。
        let mut initial = VersionEdit::new();
        initial
            .set_comparator("mulan.BytewiseComparator")
            .set_log_number(log_number.0)
            .set_next_file_number(id_gen.next_number())
            .set_last_sequence(0);
        version_set.write_new_version(&initial)?;
        write_current(dir, manifest_number)?;
        // 打开首个 WAL。
        let wal = WalWriter::create(&log_path(dir, log_number))?;
        Ok(Db {
            dir: dir.to_path_buf(),
            version_set,
            memtable: MemTable::new(),
            wal,
            id_gen,
            wal_number: log_number,
            options,
        })
    }

    /// 写入一个键值对。先落 MemTable 再追加 WAL（seq 由 MemTable 分配，WAL 复用同 seq）。
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(MulanError::InvalidArgument("key must be non-empty".into()));
        }
        self.memtable.put(key, value);
        let seq = self.memtable.sequence();
        let entry = encode_entry(ValueType::Put, seq, key, value);
        self.wal.add_record(&entry)?;
        self.maybe_flush()
    }

    /// 删除一个键：写入删除标记。
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(MulanError::InvalidArgument("key must be non-empty".into()));
        }
        self.memtable.delete(key);
        let seq = self.memtable.sequence();
        let entry = encode_entry(ValueType::Delete, seq, key, &[]);
        self.wal.add_record(&entry)?;
        self.maybe_flush()
    }

    /// 读取 key 的最新值。查找顺序：MemTable → L0 SSTable（从新到旧）。
    ///
    /// 每一层若"找到该 user_key 的最新版本"即停止：Put 返回值、Delete 返回 None。
    /// 仅当某层"无此 user_key"时才继续查下层——这是 delete 标记能屏蔽旧版本的关键。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get_entry(key)? {
            Some((ValueType::Put, v)) => return Ok(Some(v)),
            Some((ValueType::Delete, _)) => return Ok(None),
            None => {}
        }
        let version = self.version_set.current();
        // L0 文件按 flush 顺序追加，新文件在末尾；从新到旧 = 逆序遍历。
        // L0 区间可能重叠，故每个都要查，命中（Put 或 Delete）即停止。
        for meta in version.files_at(0).iter().rev() {
            let reader = TableReader::open(&sst_path(&self.dir, meta.number))?;
            if let Some((vtype, value)) = reader.get_entry(key) {
                return Ok(match vtype {
                    ValueType::Put => Some(value.to_vec()),
                    ValueType::Delete => None,
                });
            }
        }
        Ok(None)
    }

    /// 若 MemTable 攒够阈值则 flush。
    fn maybe_flush(&mut self) -> Result<()> {
        if self.memtable.num_entries() >= self.options.memtable_flush_entries {
            self.flush_memtable()?;
        }
        Ok(())
    }

    /// 把当前 MemTable 刷成 L0 SSTable，写 VersionEdit 提交，切换到新 WAL。
    ///
    /// 提交点：manifest append 成功后，新 SSTable 即"生效"。此后崩溃恢复会回放出它。
    /// append 前崩溃 → 新 SSTable 是孤儿，恢复时清理；append 后但切 WAL 前崩溃 → 旧 WAL
    /// 仍可回放，恢复时把旧 WAL 内容再 flush 一次（由 M4.5 处理）。
    fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.num_entries() == 0 {
            return Ok(());
        }
        let frozen_seq = self.memtable.sequence();
        // 冻结当前 MemTable，换上从 frozen_seq 继续递增的新 MemTable（保证全局 seq 单调）。
        let frozen = std::mem::replace(
            &mut self.memtable,
            MemTable::with_initial_sequence(frozen_seq),
        );

        // 分配 SSTable 编号并落盘。
        let sst_number = self.id_gen.new_file_number();
        let path = sst_path(&self.dir, sst_number);
        let FlushResult {
            num_entries: _,
            smallest,
            largest,
        } = frozen.flush_to_sstable_with_bounds(&path)?;
        let file_size = std::fs::metadata(&path)?.len();
        let (smallest, largest) = match (smallest, largest) {
            (Some(s), Some(l)) => (s, l),
            _ => {
                return Err(MulanError::Corrupted(
                    "flushed memtable had no entries".into(),
                ))
            }
        };
        let meta = FileMetaData::new(sst_number, file_size, smallest, largest);

        // 分配新 WAL 编号，构造提交 edit。
        let new_log_number = self.id_gen.new_file_number();
        let mut edit = VersionEdit::new();
        edit.add_file(0, meta)
            .set_log_number(new_log_number.0)
            .set_next_file_number(self.id_gen.next_number())
            .set_last_sequence(frozen_seq);
        // 提交：append manifest + 切 version。
        self.version_set.write_new_version(&edit)?;

        // 切换 WAL：旧 WAL 内容已落 SSTable，新写入走新 WAL。
        let _ = self.wal.flush();
        self.wal = WalWriter::create(&log_path(&self.dir, new_log_number))?;
        self.wal_number = new_log_number;
        Ok(())
    }

    /// 当前 WAL 编号（测试/调试用）。
    pub fn wal_number(&self) -> FileNumber {
        self.wal_number
    }

    /// 当前 Version 的 Arc（测试用）。
    pub fn current_version(&self) -> std::sync::Arc<crate::version::Version> {
        self.version_set.current()
    }
}

/// 清理孤儿文件：删除不属于当前 Version 的 SSTable、非当前 WAL、旧 Manifest、残留 tmp。
///
/// 典型来源：flush 写到一半崩溃留下的半截 SSTable（manifest 未记录）、已被新 Version
/// 替换掉的旧 SSTable/WAL/Manifest。删除失败不阻断 open（孤儿文件不影响正确性，仅占空间）。
fn remove_obsolete_files(dir: &Path, version_set: &VersionSet) -> Result<()> {
    // 收集当前 Version 引用的所有 SSTable 编号。
    let version = version_set.current();
    let mut live_sst: HashSet<FileNumber> = HashSet::new();
    for level in 0..NUM_LEVELS {
        for f in version.files_at(level) {
            live_sst.insert(f.number);
        }
    }
    let live_log = FileNumber(version_set.log_number);
    let live_manifest = version_set.manifest_number;

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = match entry.file_name().to_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        let to_remove = if let Some(num) = parse_sst_name(&name) {
            !live_sst.contains(&num)
        } else if let Some(num) = parse_log_name(&name) {
            num != live_log
        } else if let Some(num) = parse_manifest_name(&name) {
            num != live_manifest
        } else if name == CURRENT_NAME {
            false
        } else if name == format!("{CURRENT_NAME}.{TMP_SUFFIX}") {
            // CURRENT 写入的中转文件残留，清理。
            true
        } else {
            // 其他文件（未来 LOCK 等）保留。
            false
        };
        if to_remove {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

impl Drop for Db {
    fn drop(&mut self) {
        // 正常关闭时刷盘 WAL，保证已写入数据落盘。
        let _ = self.wal.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_meta::{manifest_path, FileNumber};
    use std::path::PathBuf;

    fn tmp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "mulan-db-test-{}-{}-{}",
            std::process::id(),
            label,
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn small_options() -> Options {
        Options {
            memtable_flush_entries: 5,
        }
    }

    #[test]
    fn open_new_creates_initial_files() {
        let dir = tmp_dir("new");
        {
            let _db = Db::open(&dir, small_options()).unwrap();
        }
        // 初始：MANIFEST-000001 + 000002.log + CURRENT。
        assert!(manifest_path(&dir, FileNumber(1)).exists());
        assert!(log_path(&dir, FileNumber(2)).exists());
        assert!(current_path(&dir).exists());
    }

    #[test]
    fn open_rejects_non_empty_without_current() {
        let dir = tmp_dir("nonempty");
        std::fs::write(dir.join("junk.bin"), b"x").unwrap();
        assert!(Db::open(&dir, small_options()).is_err());
    }

    #[test]
    fn put_get_round_trip_within_memtable() {
        let dir = tmp_dir("mem");
        let mut db = Db::open(&dir, small_options()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn flush_produces_sst_and_new_wal() {
        let dir = tmp_dir("flush");
        let mut db = Db::open(&dir, small_options()).unwrap();
        let first_wal = db.wal_number();
        // 写 6 条触发一次 flush（阈值 5）。
        for i in 0..6 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        // 应产生一个 L0 SSTable（000003.sst）且 WAL 已切换。
        assert!(sst_path(&dir, FileNumber(3)).exists());
        assert_ne!(db.wal_number(), first_wal);
        assert_eq!(db.current_version().num_files(0), 1);
    }

    #[test]
    fn get_reads_across_flushed_sstable() {
        let dir = tmp_dir("readflush");
        let mut db = Db::open(&dir, small_options()).unwrap();
        // 写满一个 memtable 触发 flush，数据落到 SSTable。
        for i in 0..5 {
            db.put(format!("k{i}").as_bytes(), format!("v{i}").as_bytes())
                .unwrap();
        }
        // 再写一条触发 flush（第 6 条使 entries 达到阈值）。
        db.put(b"trigger", b"x").unwrap();
        // 此时 k0..k4 已在 SSTable，memtable 已清空。
        for i in 0..5 {
            assert_eq!(
                db.get(format!("k{i}").as_bytes()).unwrap(),
                Some(format!("v{i}").into_bytes()),
                "k{i} should be readable from SSTable"
            );
        }
    }

    #[test]
    fn delete_tombstone_persists_across_flush() {
        let dir = tmp_dir("delete");
        let mut db = Db::open(&dir, small_options()).unwrap();
        db.put(b"k", b"v").unwrap();
        for i in 0..4 {
            db.put(format!("pad{i}").as_bytes(), b"x").unwrap();
        }
        // 5 条已满，下一条 delete 触发 flush（含 delete 标记）。
        db.delete(b"k").unwrap();
        // k 的删除标记已落 SSTable。
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn multi_version_latest_wins_across_flushes() {
        let dir = tmp_dir("multiver");
        let mut db = Db::open(&dir, small_options()).unwrap();
        db.put(b"k", b"v1").unwrap();
        // 填满并 flush，v1 落第一个 SSTable。
        for i in 0..4 {
            db.put(format!("pad1-{i}").as_bytes(), b"x").unwrap();
        }
        // pad1-0..3 + k + trigger=6 条，第6条触发 flush。
        db.put(b"trig1", b"x").unwrap();
        assert_eq!(db.current_version().num_files(0), 1);

        // 新 memtable 写 k=v2，再 flush 落第二个 SSTable。
        db.put(b"k", b"v2").unwrap();
        for i in 0..4 {
            db.put(format!("pad2-{i}").as_bytes(), b"x").unwrap();
        }
        db.put(b"trig2", b"x").unwrap();
        assert_eq!(db.current_version().num_files(0), 2);

        // get 应返回最新版本 v2（第二个 SSTable 比第一个新，先查到）。
        assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn continues_writing_to_new_wal_after_flush() {
        let dir = tmp_dir("continue");
        let mut db = Db::open(&dir, small_options()).unwrap();
        for i in 0..6 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        // flush 后继续写，新数据应进新 WAL + 新 memtable，且可读。
        db.put(b"after", b"flush").unwrap();
        assert_eq!(db.get(b"after").unwrap(), Some(b"flush".to_vec()));
        // 之前的数据仍可读。
        assert_eq!(db.get(b"k0").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn empty_key_rejected() {
        let dir = tmp_dir("emptykey");
        let mut db = Db::open(&dir, small_options()).unwrap();
        assert!(db.put(b"", b"v").is_err());
        assert!(db.delete(b"").is_err());
    }

    // ===== Step 4.5: 恢复 + 孤儿清理 =====

    #[test]
    fn reopen_recovers_all_flushed_data() {
        let dir = tmp_dir("reopen");
        // 第一轮：写入并多次 flush。
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            for i in 0..30 {
                db.put(format!("k{i:02}").as_bytes(), format!("v{i}").as_bytes())
                    .unwrap();
            }
            // 多次 flush 产生多个 L0 SSTable。
            assert!(db.current_version().num_files(0) >= 1);
        } // drop → flush WAL
          // 第二轮：重新打开，已 flush 的数据应全部可读。
        {
            let db = Db::open(&dir, small_options()).unwrap();
            for i in 0..30 {
                assert_eq!(
                    db.get(format!("k{i:02}").as_bytes()).unwrap(),
                    Some(format!("v{i}").into_bytes()),
                    "k{i:02} lost after reopen"
                );
            }
        }
    }

    #[test]
    fn reopen_recovers_unflushed_wal_entries() {
        let dir = tmp_dir("wal");
        // 写几条不足以触发 flush 的数据，WAL 有内容但未落 SSTable。
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            db.put(b"a", b"1").unwrap();
            db.put(b"b", b"2").unwrap();
            db.put(b"c", b"3").unwrap();
            assert_eq!(db.current_version().num_files(0), 0);
        }
        // 重开：回放 WAL 重建 MemTable，数据应可读（且会被 flush 成 L0）。
        {
            let db = Db::open(&dir, small_options()).unwrap();
            assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
            assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
            assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()));
        }
    }

    #[test]
    fn reopen_then_continue_writing() {
        let dir = tmp_dir("continue2");
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            for i in 0..10 {
                db.put(format!("old{i}").as_bytes(), b"x").unwrap();
            }
        }
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            // 恢复后继续写新数据。
            db.put(b"new", b"y").unwrap();
            assert_eq!(db.get(b"new").unwrap(), Some(b"y".to_vec()));
            // 旧数据仍在。
            assert_eq!(db.get(b"old0").unwrap(), Some(b"x".to_vec()));
        }
        // 第三轮再开，新写入也持久化。
        {
            let db = Db::open(&dir, small_options()).unwrap();
            assert_eq!(db.get(b"new").unwrap(), Some(b"y".to_vec()));
            assert_eq!(db.get(b"old9").unwrap(), Some(b"x".to_vec()));
        }
    }

    #[test]
    fn orphan_sst_is_cleaned_on_open() {
        let dir = tmp_dir("orphansst");
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            for i in 0..6 {
                db.put(format!("k{i}").as_bytes(), b"v").unwrap();
            }
        }
        // 人为放一个不在 manifest 的孤儿 SSTable。
        let orphan = sst_path(&dir, FileNumber(999));
        std::fs::write(&orphan, b"garbage").unwrap();
        assert!(orphan.exists());

        // 重开应清掉孤儿。
        let _db = Db::open(&dir, small_options()).unwrap();
        assert!(!orphan.exists(), "orphan sst should be removed");
    }

    #[test]
    fn obsolete_wal_is_cleaned_on_open() {
        let dir = tmp_dir("orphwal");
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            // 多次 flush 切换 WAL，产生旧 WAL。
            for i in 0..20 {
                db.put(format!("k{i}").as_bytes(), b"v").unwrap();
            }
        }
        // 重开后，只应保留当前 WAL（recover 后可能再切一次）。
        let _db = Db::open(&dir, small_options()).unwrap();
        let logs: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| {
                let e = e.unwrap();
                e.file_name().to_str().map(|s| s.to_string())
            })
            .filter(|s| parse_log_name(s).is_some())
            .collect();
        // 至少 1 个（当前 WAL），但不应该保留全部历史 WAL。
        assert!(!logs.is_empty(), "current WAL must remain");
        // 旧 WAL 应被清理：第一轮若切了 N 个 WAL，重开后只留 1-2 个。
        // 用宽松断言：log 数量不应随历史无限增长。
        assert!(
            logs.len() <= 2,
            "obsolete WALs should be cleaned, found {logs:?}"
        );
    }

    #[test]
    fn crash_before_manifest_sync_loses_uncommitted_sst() {
        // 模拟：flush 写完 SSTable 但 manifest 未记录（人为造一个孤儿 SSTable + 正常 manifest）。
        let dir = tmp_dir("crashmanifest");
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            db.put(b"k", b"v").unwrap();
        }
        // 正常关闭后数据在。现在人为加一个孤儿 SSTable（模拟 flush 后崩溃、manifest 没写）。
        let orphan = sst_path(&dir, FileNumber(500));
        std::fs::write(&orphan, b"uncommitted").unwrap();
        // 重开：孤儿被清理，但已提交的数据仍在。
        let db = Db::open(&dir, small_options()).unwrap();
        assert!(!orphan.exists());
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn reopen_preserves_delete_across_restart() {
        let dir = tmp_dir("delpersist");
        {
            let mut db = Db::open(&dir, small_options()).unwrap();
            db.put(b"k", b"v").unwrap();
            for i in 0..4 {
                db.put(format!("pad{i}").as_bytes(), b"x").unwrap();
            }
            db.delete(b"k").unwrap(); // 触发 flush，delete 标记落 SSTable
        }
        let db = Db::open(&dir, small_options()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None, "delete must survive restart");
    }
}
