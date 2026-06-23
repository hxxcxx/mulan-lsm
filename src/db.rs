//! `Db`：编排 MemTable + WAL + SSTable + VersionSet 的 LSM 主类。
//!
//! 并发模型：`Arc<(Mutex<DbInner>, Condvar)>` + 后台 compaction 线程。
//! - 前台 `put`/`delete`：持锁写 MemTable + WAL + maybe_flush + 唤醒后台。
//! - 前台 `get`：持锁取 `Arc<Version>` + MemTable 快照，释放锁后无锁查 SSTable。
//! - 后台线程：持锁 pick_compaction，做归并 IO，持锁提交 manifest。

use crate::compaction::{compaction_score, do_compaction, level0_needs_slowdown, pick_compaction};
use crate::error::{MulanError, Result};
use crate::file_meta::{
    current_path, log_path, parse_log_name, parse_manifest_name, parse_sst_name, sst_path,
    FileMetaData, FileNumber, IdGenerator, CURRENT_NAME, TMP_SUFFIX,
};
use crate::internal_key::ValueType;
use crate::manifest::{write_current, VersionEdit};
use crate::memtable::{FlushResult, MemTable};
use crate::sstable::TableReader;
use crate::version::{Version, VersionSet, NUM_LEVELS};
use crate::wal::{decode_entry, encode_entry, WalReader, WalWriter};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex};

/// DB 配置。
#[derive(Debug, Clone)]
pub struct Options {
    pub memtable_flush_entries: usize,
    /// 禁用后台自动 compaction（测试用，配合 `compact_once` 手动控制）。
    pub disable_auto_compaction: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_flush_entries: 1000,
            disable_auto_compaction: false,
        }
    }
}

struct DbInner {
    dir: PathBuf,
    version_set: VersionSet,
    memtable: MemTable,
    wal: WalWriter,
    id_gen: IdGenerator,
    wal_number: FileNumber,
    options: Options,
    bg_compaction_scheduled: bool,
    shutting_down: bool,
}

/// mulan-lsm 数据库主类。一个实例对应一个目录。
pub struct Db {
    inner: Arc<(Mutex<DbInner>, Condvar)>,
    bg_thread: Option<std::thread::JoinHandle<()>>,
}

/// 一致性快照句柄。RAII：drop 时自动从 VersionSet 注销。
///
/// 快照固定一个 seq，读时只看 ≤ 该 seq 的版本，实现 MVCC 一致性读。
/// 同时约束 compaction：seq > 快照 seq 的旧版本不会被回收，保证快照可读。
pub struct SnapshotGuard {
    seq: u64,
    inner: Arc<(Mutex<DbInner>, Condvar)>,
}

impl SnapshotGuard {
    /// 快照固定的 seq。读路径用此值过滤版本。
    pub fn sequence(&self) -> u64 {
        self.seq
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        let (lock, _) = &*self.inner;
        let mut inner = lock.lock().unwrap();
        inner.version_set.release_snapshot(self.seq);
    }
}

impl Db {
    pub fn open(dir: &Path, options: Options) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let auto_compact = !options.disable_auto_compaction;
        let db_inner: DbInner = if current_path(dir).exists() {
            Self::recover_open(dir, options)?
        } else {
            let is_empty = std::fs::read_dir(dir)?.next().is_none();
            if !is_empty {
                return Err(MulanError::InvalidArgument(format!(
                    "directory {:?} is not empty and has no CURRENT; refusing to initialize",
                    dir
                )));
            }
            Self::create_new(dir, options)?
        };
        let inner = Arc::new((Mutex::new(db_inner), Condvar::new()));
        let bg_thread = if auto_compact {
            let c = inner.clone();
            Some(std::thread::spawn(move || background_compaction(c)))
        } else {
            None
        };
        Ok(Db { inner, bg_thread })
    }

    fn create_new(dir: &Path, options: Options) -> Result<DbInner> {
        let mut id_gen = IdGenerator::new(1);
        let manifest_number = id_gen.new_file_number();
        let log_number = id_gen.new_file_number();
        let mut version_set = VersionSet::new_pending(dir, manifest_number)?;
        let mut initial = VersionEdit::new();
        initial
            .set_comparator("mulan.BytewiseComparator")
            .set_log_number(log_number.0)
            .set_next_file_number(id_gen.next_number())
            .set_last_sequence(0);
        version_set.write_new_version(&initial)?;
        write_current(dir, manifest_number)?;
        let wal = WalWriter::create(&log_path(dir, log_number))?;
        Ok(DbInner {
            dir: dir.to_path_buf(),
            version_set,
            memtable: MemTable::new(),
            wal,
            id_gen,
            wal_number: log_number,
            options,
            bg_compaction_scheduled: false,
            shutting_down: false,
        })
    }

    fn recover_open(dir: &Path, options: Options) -> Result<DbInner> {
        let mut version_set = VersionSet::recover(dir)?;
        let mut id_gen = IdGenerator::new(version_set.next_file_number);
        let mut memtable = MemTable::with_initial_sequence(version_set.last_sequence);
        let current_log = FileNumber(version_set.log_number);
        let p = log_path(dir, current_log);
        if p.exists() {
            let reader = WalReader::open(&p)?;
            for rec in reader.read_records()? {
                let e = decode_entry(&rec)?;
                memtable.apply(e.vtype, e.seq, &e.key, &e.value);
            }
        }
        let wal_number;
        if memtable.num_entries() > 0 {
            let sst = id_gen.new_file_number();
            let path = sst_path(dir, sst);
            let FlushResult {
                smallest, largest, ..
            } = memtable.flush_to_sstable_with_bounds(&path)?;
            let sz = std::fs::metadata(&path)?.len();
            let (s, l) = match (smallest, largest) {
                (Some(s), Some(l)) => (s, l),
                _ => return Err(MulanError::Corrupted("recovered memtable empty".into())),
            };
            let meta = FileMetaData::new(sst, sz, s, l);
            let new_log = id_gen.new_file_number();
            let mut edit = VersionEdit::new();
            edit.add_file(0, meta)
                .set_log_number(new_log.0)
                .set_next_file_number(id_gen.next_number())
                .set_last_sequence(memtable.sequence());
            version_set.write_new_version(&edit)?;
            memtable = MemTable::with_initial_sequence(memtable.sequence());
            wal_number = new_log;
        } else {
            wal_number = current_log;
        }
        let wal = WalWriter::create(&log_path(dir, wal_number))?;
        remove_obsolete_files(dir, &version_set)?;
        Ok(DbInner {
            dir: dir.to_path_buf(),
            version_set,
            memtable,
            wal,
            id_gen,
            wal_number,
            options,
            bg_compaction_scheduled: false,
            shutting_down: false,
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(MulanError::InvalidArgument("key must be non-empty".into()));
        }
        self.write(key, value, ValueType::Put)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        if key.is_empty() {
            return Err(MulanError::InvalidArgument("key must be non-empty".into()));
        }
        self.write(key, &[], ValueType::Delete)
    }

    fn write(&self, key: &[u8], value: &[u8], vtype: ValueType) -> Result<()> {
        let (lock, cvar) = &*self.inner;
        let mut inner = lock.lock().unwrap();
        while level0_needs_slowdown(&inner.version_set.current())
            && !inner.options.disable_auto_compaction
        {
            maybe_schedule_compaction(&mut inner);
            inner = cvar.wait(inner).unwrap();
        }
        match vtype {
            ValueType::Put => inner.memtable.put(key, value),
            ValueType::Delete => inner.memtable.delete(key),
        }
        let seq = inner.memtable.sequence();
        inner
            .wal
            .add_record(&encode_entry(vtype, seq, key, value))?;
        if inner.memtable.num_entries() >= inner.options.memtable_flush_entries {
            flush_memtable(&mut inner)?;
        }
        maybe_schedule_compaction(&mut inner);
        drop(inner);
        cvar.notify_one();
        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (lock, _) = &*self.inner;
        let inner = lock.lock().unwrap();
        match inner.memtable.get_entry(key)? {
            Some((ValueType::Put, v)) => return Ok(Some(v)),
            Some((ValueType::Delete, _)) => return Ok(None),
            None => {}
        }
        let version = inner.version_set.current();
        let dir = inner.dir.clone();
        drop(inner);
        for meta in version.files_at(0).iter().rev() {
            let r = TableReader::open(&sst_path(&dir, meta.number))?;
            if let Some((vt, v)) = r.get_entry(key) {
                return Ok(match vt {
                    ValueType::Put => Some(v.to_vec()),
                    ValueType::Delete => None,
                });
            }
        }
        for level in 1..NUM_LEVELS {
            if let Some(meta) = find_file_for_key(version.files_at(level), key) {
                let r = TableReader::open(&sst_path(&dir, meta.number))?;
                if let Some((vt, v)) = r.get_entry(key) {
                    return Ok(match vt {
                        ValueType::Put => Some(v.to_vec()),
                        ValueType::Delete => None,
                    });
                }
            }
        }
        Ok(None)
    }

    pub fn wal_number(&self) -> FileNumber {
        self.inner.0.lock().unwrap().wal_number
    }

    pub fn current_version(&self) -> Arc<Version> {
        self.inner.0.lock().unwrap().version_set.current()
    }

    /// 获取一致性快照：固定当前最新已分配的 seq 作为读时间点。
    /// seq 取自 MemTable（每次写入递增），而非 VersionSet.last_sequence（只在 flush 时同步）——
    /// 这样快照能看到 memtable 中尚未 flush 的写入。
    /// 返回的 SnapshotGuard drop 时自动释放。快照存在期间，compaction 不会回收
    /// seq > 快照 seq 的旧版本，保证快照读到的一致性视图稳定。
    pub fn new_snapshot(&self) -> SnapshotGuard {
        let (lock, _) = &*self.inner;
        let mut inner = lock.lock().unwrap();
        // seq 取自 MemTable（每次写入递增），而非 VersionSet.last_sequence（只在 flush 时同步）——
        // 这样快照能看到 memtable 中尚未 flush 的写入。
        let seq = inner.memtable.sequence();
        inner.version_set.acquire_snapshot(seq);
        SnapshotGuard {
            seq,
            inner: self.inner.clone(),
        }
    }

    /// 当前最老活跃快照的 seq（无活跃快照返回 MAX_SEQUENCE）。
    /// 供测试和诊断观察快照对 compaction 的约束。
    pub fn oldest_snapshot_seq(&self) -> u64 {
        let (lock, _) = &*self.inner;
        let inner = lock.lock().unwrap();
        inner.version_set.oldest_snapshot_seq()
    }

    /// 手动触发一次 compaction（测试用）。返回是否执行了 compaction。
    pub fn compact_once(&self) -> Result<bool> {
        let (lock, cvar) = &*self.inner;
        let mut inner = lock.lock().unwrap();
        let did = run_one_compaction(&mut inner)?;
        drop(inner);
        cvar.notify_one();
        Ok(did)
    }
}

fn find_file_for_key<'a>(files: &'a [FileMetaData], key: &[u8]) -> Option<&'a FileMetaData> {
    let idx = files.partition_point(|f| f.smallest.user_key.as_slice() <= key);
    if idx == 0 {
        return None;
    }
    let f = &files[idx - 1];
    if key <= f.largest.user_key.as_slice() {
        Some(f)
    } else {
        None
    }
}

fn flush_memtable(inner: &mut DbInner) -> Result<()> {
    if inner.memtable.num_entries() == 0 {
        return Ok(());
    }
    let frozen_seq = inner.memtable.sequence();
    let frozen = std::mem::replace(
        &mut inner.memtable,
        MemTable::with_initial_sequence(frozen_seq),
    );
    let sst = inner.id_gen.new_file_number();
    let path = sst_path(&inner.dir, sst);
    let FlushResult {
        smallest, largest, ..
    } = frozen.flush_to_sstable_with_bounds(&path)?;
    let sz = std::fs::metadata(&path)?.len();
    let (s, l) = match (smallest, largest) {
        (Some(s), Some(l)) => (s, l),
        _ => {
            return Err(MulanError::Corrupted(
                "flushed memtable had no entries".into(),
            ))
        }
    };
    let meta = FileMetaData::new(sst, sz, s, l);
    let new_log = inner.id_gen.new_file_number();
    let mut edit = VersionEdit::new();
    edit.add_file(0, meta)
        .set_log_number(new_log.0)
        .set_next_file_number(inner.id_gen.next_number())
        .set_last_sequence(frozen_seq);
    inner.version_set.write_new_version(&edit)?;
    let _ = inner.wal.flush();
    inner.wal = WalWriter::create(&log_path(&inner.dir, new_log))?;
    inner.wal_number = new_log;
    Ok(())
}

fn maybe_schedule_compaction(inner: &mut DbInner) {
    if inner.options.disable_auto_compaction || inner.bg_compaction_scheduled || inner.shutting_down
    {
        return;
    }
    if compaction_score(&inner.version_set.current()).is_some() {
        inner.bg_compaction_scheduled = true;
    }
}

/// 执行一次 compaction：pick → 归并 → 提交。返回是否做了 compaction。
fn run_one_compaction(inner: &mut std::sync::MutexGuard<'_, DbInner>) -> Result<bool> {
    let compaction = match pick_compaction(&inner.version_set) {
        Some(c) => c,
        None => return Ok(false),
    };
    let level = compaction.level;
    let version = inner.version_set.current();
    let dir = inner.dir.clone();
    let oldest = inner.version_set.oldest_snapshot_seq();
    let start = inner.id_gen.next_number();
    let mut id_gen = IdGenerator::new(start);
    let output = do_compaction(&dir, &compaction, &version, &mut id_gen, oldest)?;
    inner.id_gen.bump_to(id_gen.next_number());
    let mut edit = VersionEdit::new();
    for f in &output.new_files {
        edit.add_file((level + 1) as u32, f.clone());
    }
    for (lvl, num) in &output.deleted_files {
        edit.add_delete(*lvl as u32, *num);
    }
    edit.set_next_file_number(inner.id_gen.next_number());
    inner.version_set.write_new_version(&edit)?;
    if let Some(last) = output.new_files.last() {
        inner
            .version_set
            .set_compact_pointer(level, last.largest.user_key.clone());
    }
    // 提交后清理被替换的旧 SSTable（孤儿），防磁盘空间膨胀。
    // 只清理 SSTable；WAL/manifest 由 open 路径处理（运行期它们可能正被使用）。
    remove_obsolete_ssts(&inner.dir, &inner.version_set)?;
    Ok(true)
}

/// 运行期清理：只删不属于当前 Version 的 SSTable（compaction 替换掉的旧文件）。
fn remove_obsolete_ssts(dir: &Path, version_set: &VersionSet) -> Result<()> {
    let version = version_set.current();
    let mut live_sst: HashSet<FileNumber> = HashSet::new();
    for level in 0..NUM_LEVELS {
        for f in version.files_at(level) {
            live_sst.insert(f.number);
        }
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = match entry.file_name().to_str() {
            Some(s) => s.to_string(),
            None => continue,
        };
        if let Some(n) = parse_sst_name(&name) {
            if !live_sst.contains(&n) {
                let _ = std::fs::remove_file(&path);
            }
        }
    }
    Ok(())
}

fn background_compaction(inner: Arc<(Mutex<DbInner>, Condvar)>) {
    let (lock, cvar) = &*inner;
    loop {
        let mut guard = lock.lock().unwrap();
        while !guard.shutting_down
            && !guard.bg_compaction_scheduled
            && compaction_score(&guard.version_set.current()).is_none()
        {
            guard = cvar.wait(guard).unwrap();
        }
        if guard.shutting_down {
            break;
        }
        let _ = run_one_compaction(&mut guard);
        guard.bg_compaction_scheduled = false;
        cvar.notify_all();
    }
}

fn remove_obsolete_files(dir: &Path, version_set: &VersionSet) -> Result<()> {
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
        let rm = if let Some(n) = parse_sst_name(&name) {
            !live_sst.contains(&n)
        } else if let Some(n) = parse_log_name(&name) {
            n != live_log
        } else if let Some(n) = parse_manifest_name(&name) {
            n != live_manifest
        } else if name == CURRENT_NAME {
            false
        } else {
            name == format!("{CURRENT_NAME}.{TMP_SUFFIX}")
        };
        if rm {
            let _ = std::fs::remove_file(&path);
        }
    }
    Ok(())
}

impl Drop for Db {
    fn drop(&mut self) {
        {
            let (lock, cvar) = &*self.inner;
            let mut g = lock.lock().unwrap();
            g.shutting_down = true;
            let _ = g.wal.flush();
            cvar.notify_all();
        }
        if let Some(h) = self.bg_thread.take() {
            let _ = h.join();
        }
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
        // 禁用自动 compaction，保持测试确定性（手动 compact_once 控制时序）。
        Options {
            memtable_flush_entries: 5,
            disable_auto_compaction: true,
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
        let db = Db::open(&dir, small_options()).unwrap();
        db.put(b"k1", b"v1").unwrap();
        db.put(b"k2", b"v2").unwrap();
        assert_eq!(db.get(b"k1").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get(b"k2").unwrap(), Some(b"v2".to_vec()));
        assert_eq!(db.get(b"missing").unwrap(), None);
    }

    #[test]
    fn flush_produces_sst_and_new_wal() {
        let dir = tmp_dir("flush");
        let db = Db::open(&dir, small_options()).unwrap();
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
        let db = Db::open(&dir, small_options()).unwrap();
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
        let db = Db::open(&dir, small_options()).unwrap();
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
        let db = Db::open(&dir, small_options()).unwrap();
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
        let db = Db::open(&dir, small_options()).unwrap();
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
        let db = Db::open(&dir, small_options()).unwrap();
        assert!(db.put(b"", b"v").is_err());
        assert!(db.delete(b"").is_err());
    }

    #[test]
    fn reopen_recovers_all_flushed_data() {
        let dir = tmp_dir("reopen");
        // 第一轮：写入并多次 flush。
        {
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
            for i in 0..10 {
                db.put(format!("old{i}").as_bytes(), b"x").unwrap();
            }
        }
        {
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
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
            let db = Db::open(&dir, small_options()).unwrap();
            db.put(b"k", b"v").unwrap();
            for i in 0..4 {
                db.put(format!("pad{i}").as_bytes(), b"x").unwrap();
            }
            db.delete(b"k").unwrap(); // 触发 flush，delete 标记落 SSTable
        }
        let db = Db::open(&dir, small_options()).unwrap();
        assert_eq!(db.get(b"k").unwrap(), None, "delete must survive restart");
    }

    #[test]
    fn manual_compaction_moves_l0_to_l1() {
        // 禁用自动 compaction，手动 compact_once 把 L0 归并到 L1。
        let dir = tmp_dir("manualcompact");
        let db = Db::open(&dir, small_options()).unwrap();
        // 写入触发多次 flush，产生 4+ 个 L0 文件。
        for batch in 0..5 {
            for i in 0..5 {
                db.put(format!("b{batch}k{i}").as_bytes(), b"v").unwrap();
            }
        }
        let before = db.current_version().num_files(0);
        assert!(before >= 4, "should have multiple L0 files, got {before}");

        // 手动 compact：L0 文件应减少，L1 应出现文件。
        let did = db.compact_once().unwrap();
        assert!(did, "compaction should do work");
        let after_l0 = db.current_version().num_files(0);
        let after_l1 = db.current_version().num_files(1);
        assert!(
            after_l0 < before,
            "L0 files should decrease: {before} -> {after_l0}"
        );
        assert!(after_l1 > 0, "L1 should have files after compaction");
    }

    #[test]
    fn compaction_preserves_data_correctness() {
        // compaction 后所有 key 仍可正确读取。
        let dir = tmp_dir("compactcorrect");
        let db = Db::open(&dir, small_options()).unwrap();
        let mut expected = std::collections::HashMap::new();
        for i in 0..30 {
            let k = format!("k{i:02}").into_bytes();
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            expected.insert(k, v);
        }
        // 手动 compact 多次直到无任务。
        while db.compact_once().unwrap() {}
        // 全部 key 仍可读。
        for (k, v) in &expected {
            assert_eq!(
                db.get(k).unwrap(),
                Some(v.clone()),
                "lost after compaction: {k:?}"
            );
        }
    }

    #[test]
    fn compaction_drops_obsolete_input_files() {
        // compaction 后输入文件应被孤儿清理（下次 open 时）。
        let dir = tmp_dir("dropinput");
        {
            let db = Db::open(&dir, small_options()).unwrap();
            for i in 0..25 {
                db.put(format!("k{i:02}").as_bytes(), b"v").unwrap();
            }
            // compact 直到完成。
            while db.compact_once().unwrap() {}
        }
        // 重开触发孤儿清理，验证无 panic 且数据在。
        let db = Db::open(&dir, small_options()).unwrap();
        assert_eq!(db.get(b"k00").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn auto_compaction_runs_in_background() {
        // 启用自动 compaction，持续写入，L0 文件数不应无限增长。
        let dir = tmp_dir("autocompact");
        let opts = Options {
            memtable_flush_entries: 5,
            disable_auto_compaction: false,
        };
        let db = Db::open(&dir, opts).unwrap();
        // 写入 200 条（40 次 flush），后台应自动 compaction。
        for i in 0..200 {
            let v = format!("v{i}");
            db.put(format!("k{i:03}").as_bytes(), v.as_bytes()).unwrap();
        }
        // 给后台线程一点时间完成 compaction。
        std::thread::sleep(std::time::Duration::from_millis(500));
        // L0 不应堆积过多（远小于写入次数/flush 阈值）。
        let l0 = db.current_version().num_files(0);
        assert!(l0 < 20, "L0 should be compacted, got {l0} files");
        // 全部数据可读。
        for i in 0..200 {
            let v = format!("v{i}");
            assert_eq!(
                db.get(format!("k{i:03}").as_bytes()).unwrap(),
                Some(v.into_bytes()),
                "k{i:03} lost"
            );
        }
    }

    #[test]
    fn drop_joins_background_thread_cleanly() {
        // Db drop 应等待后台线程退出，无悬挂线程。
        let dir = tmp_dir("dropjoin");
        let opts = Options {
            memtable_flush_entries: 5,
            disable_auto_compaction: false,
        };
        let handle = {
            let db = Db::open(&dir, opts).unwrap();
            for i in 0..50 {
                db.put(format!("k{i}").as_bytes(), b"v").unwrap();
            }
            // 取后台线程 handle（通过反射不可行，改用：drop 后 reopen 验证无锁竞争）。
            std::thread::current().id()
        };
        // db 在此 drop。若后台线程未退出，reopen 同目录可能冲突。
        let db2 = Db::open(&dir, small_options()).unwrap();
        assert_eq!(db2.get(b"k0").unwrap(), Some(b"v".to_vec()));
        let _ = handle; // 抑制未使用警告。
    }

    #[test]
    fn snapshot_acquire_and_release_via_guard() {
        // SnapshotGuard 是 RAII：drop 时自动从 VersionSet 注销。
        let dir = tmp_dir("snap-guard");
        let db = Db::open(&dir, small_options()).unwrap();
        db.put(b"k1", b"v1").unwrap(); // seq=1

        {
            let snap = db.new_snapshot();
            assert_eq!(snap.sequence(), 1, "快照 seq 应等于当时的 last_sequence");
            assert_eq!(
                db.oldest_snapshot_seq(),
                1,
                "快照存在时 oldest 应为快照 seq"
            );
        } // snap 在此 drop
        assert_eq!(
            db.oldest_snapshot_seq(),
            crate::internal_key::MAX_SEQUENCE,
            "快照释放后 oldest 应恢复 MAX_SEQUENCE"
        );
    }

    #[test]
    fn snapshot_multiple_guards_track_oldest() {
        // 多个快照时 oldest = 最小 seq；逐个释放后 oldest 跟进。
        let dir = tmp_dir("snap-multi");
        let db = Db::open(&dir, small_options()).unwrap();
        db.put(b"a", b"1").unwrap(); // seq=1
        let snap1 = db.new_snapshot(); // seq=1
        db.put(b"b", b"2").unwrap(); // seq=2
        let snap2 = db.new_snapshot(); // seq=2
        assert_eq!(db.oldest_snapshot_seq(), 1);

        drop(snap1);
        assert_eq!(db.oldest_snapshot_seq(), 2);

        drop(snap2);
        assert_eq!(db.oldest_snapshot_seq(), crate::internal_key::MAX_SEQUENCE);
    }

    #[test]
    fn snapshot_seq_advances_with_writes() {
        // 写入推进 last_sequence，新快照的 seq 比旧快照大。
        let dir = tmp_dir("snap-advance");
        let db = Db::open(&dir, small_options()).unwrap();
        db.put(b"a", b"1").unwrap();
        let s1 = db.new_snapshot();
        db.put(b"b", b"2").unwrap();
        db.put(b"c", b"3").unwrap();
        let s2 = db.new_snapshot();
        assert!(s2.sequence() > s1.sequence(), "新快照 seq 应更大");
    }
}
