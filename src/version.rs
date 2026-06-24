//! `Version` / `VersionSet`：某时刻数据库状态快照 + 状态机管理。
//!
//! `Version` 不可变：每次 flush/compaction 产生新 `Version`，旧 `Version` 被 `Arc` 持有
//! 供进行中的读继续使用，写新 `Version` 时不原地改旧的。`VersionSet` 管理当前 `Version`
//! 及标量元数据（`log_number` / `next_file_number` / `last_sequence` 等）和 manifest writer，
//! 是 flush/compaction 提交状态的统一入口。

use crate::error::{MulanError, Result};
use crate::file_meta::{FileMetaData, FileNumber};
use crate::internal_key::MAX_SEQUENCE;
use crate::manifest::{recover_manifest, ManifestWriter, VersionEdit};
use std::path::Path;
use std::sync::Arc;

/// 判断文件的 user_key 区间是否与 [range_smallest, range_largest] 重叠。
/// 比较基于 user_key 字节字典序（internal key 的 user_key 部分）。
fn user_key_overlap(file: &FileMetaData, range_smallest: &[u8], range_largest: &[u8]) -> bool {
    // 文件 largest.user_key < range_smallest → 文件在区间左侧，无重叠。
    if file.largest.user_key.as_slice() < range_smallest {
        return false;
    }
    // 文件 smallest.user_key > range_largest → 文件在区间右侧，无重叠。
    if file.smallest.user_key.as_slice() > range_largest {
        return false;
    }
    true
}

/// LevelDB 默认层数：Level 0..=6。L0 文件区间可重叠，L1+ 严格不重叠（由 compaction 保证）。
pub const NUM_LEVELS: usize = 7;

/// 某时刻数据库状态：各层的 SSTable 文件列表。不可变。
#[derive(Debug, Clone)]
pub struct Version {
    files: [Vec<FileMetaData>; NUM_LEVELS],
}

impl Version {
    pub fn empty() -> Self {
        Version {
            files: std::array::from_fn(|_| Vec::new()),
        }
    }

    pub fn files_at(&self, level: usize) -> &[FileMetaData] {
        &self.files[level]
    }

    pub fn num_files(&self, level: usize) -> usize {
        self.files[level].len()
    }

    pub fn total_size(&self, level: usize) -> u64 {
        self.files[level].iter().map(|f| f.file_size).sum()
    }

    pub fn num_levels(&self) -> usize {
        NUM_LEVELS
    }

    /// 返回某层 user_key 区间 [smallest, largest] 内的所有文件。
    /// 用于 compaction 选下一层重叠文件、祖父层重叠计算。
    pub fn get_overlaps(
        &self,
        level: usize,
        range_smallest: &[u8],
        range_largest: &[u8],
    ) -> Vec<FileMetaData> {
        self.files[level]
            .iter()
            .filter(|f| user_key_overlap(f, range_smallest, range_largest))
            .cloned()
            .collect()
    }
}

/// 把一条 VersionEdit 应用到 base Version，产生新 Version。
/// 先删后增（LevelDB 顺序）；level 越界视为损坏。
fn apply_edit(base: &Version, edit: &VersionEdit) -> Result<Version> {
    let mut files = base.files.clone();
    for d in &edit.deleted_files {
        let level = d.level as usize;
        if level >= NUM_LEVELS {
            return Err(MulanError::Corrupted(format!(
                "deleted file level {level} out of range"
            )));
        }
        files[level].retain(|f| f.number != d.number);
    }
    for f in &edit.new_files {
        let level = f.level as usize;
        if level >= NUM_LEVELS {
            return Err(MulanError::Corrupted(format!(
                "new file level {level} out of range"
            )));
        }
        files[level].push(f.meta.clone());
    }
    Ok(Version { files })
}

/// 数据库状态机：当前 Version + 标量元数据 + 持续追加的 manifest writer。
pub struct VersionSet {
    current: Arc<Version>,
    pub log_number: u64,
    pub prev_log_number: u64,
    pub next_file_number: u64,
    pub last_sequence: u64,
    pub manifest_number: FileNumber,
    manifest_writer: ManifestWriter,
    /// 每层 compaction 轮转起点（user_key 字节）。内存状态，不持久化。
    /// 下次 compact 该层时从这之后开始选文件，保证 key 空间均匀压缩。
    compact_pointer: [Vec<u8>; NUM_LEVELS],
    /// 活跃快照的 seq 列表。compaction 丢弃旧版本时用它判断下限：
    /// seq > oldest_snapshot_seq 的版本可能被快照引用，不能丢。
    /// 快照数量通常很少（个位数），用 Vec 的 O(n) 删除换取零 unsafe。
    snapshots: Vec<u64>,
}

impl VersionSet {
    /// 当前 Version 的 Arc 副本。读路径持此 Arc，保证读期间对应文件不被回收。
    pub fn current(&self) -> Arc<Version> {
        self.current.clone()
    }

    /// 为新库创建 VersionSet：打开指定 manifest 的 writer，标量初始化为 0，Version 为空。
    /// 调用方随后用 `write_new_version` 写入初始 VersionEdit 并 `write_current`。
    pub fn new_pending(dir: &Path, manifest_number: FileNumber) -> Result<Self> {
        let writer = ManifestWriter::create(dir, manifest_number)?;
        Ok(VersionSet {
            current: Arc::new(Version::empty()),
            log_number: 0,
            prev_log_number: 0,
            next_file_number: 0,
            last_sequence: 0,
            manifest_number,
            manifest_writer: writer,
            compact_pointer: std::array::from_fn(|_| Vec::new()),
            snapshots: Vec::new(),
        })
    }

    /// 从已有 manifest 回放，重建 VersionSet 并重新打开 writer 继续追加。
    pub fn recover(dir: &Path) -> Result<Self> {
        let recovery = recover_manifest(dir)?;
        let mut version = Version::empty();
        let mut log_number = 0;
        let mut prev_log_number = 0;
        let mut next_file_number = 0;
        let mut last_sequence = 0;
        let mut compact_pointer: [Vec<u8>; NUM_LEVELS] = std::array::from_fn(|_| Vec::new());
        for edit in &recovery.edits {
            version = apply_edit(&version, edit)?;
            if let Some(n) = edit.log_number {
                log_number = n;
            }
            if let Some(n) = edit.prev_log_number {
                prev_log_number = n;
            }
            if let Some(n) = edit.next_file_number {
                next_file_number = n;
            }
            if let Some(s) = edit.last_sequence {
                last_sequence = s;
            }
            for (level, key) in &edit.compact_pointers {
                if (*level as usize) < NUM_LEVELS {
                    compact_pointer[*level as usize] = key.clone();
                }
            }
        }
        let writer = ManifestWriter::create(dir, recovery.manifest_number)?;
        Ok(VersionSet {
            current: Arc::new(version),
            log_number,
            prev_log_number,
            next_file_number,
            last_sequence,
            manifest_number: recovery.manifest_number,
            manifest_writer: writer,
            compact_pointer,
            snapshots: Vec::new(),
        })
    }

    /// 追加一条 VersionEdit 到 manifest 并切换 current。
    ///
    /// manifest append 成功即视为提交：此后崩溃恢复会回放出含此 edit 的状态。
    /// 返回新 current 的 Arc。
    pub fn write_new_version(&mut self, edit: &VersionEdit) -> Result<Arc<Version>> {
        self.manifest_writer.append(edit)?;
        let new_version = apply_edit(&self.current, edit)?;
        self.current = Arc::new(new_version);
        if let Some(n) = edit.log_number {
            self.log_number = n;
        }
        if let Some(n) = edit.prev_log_number {
            self.prev_log_number = n;
        }
        if let Some(n) = edit.next_file_number {
            self.next_file_number = n;
        }
        if let Some(s) = edit.last_sequence {
            self.last_sequence = s;
        }
        for (level, key) in &edit.compact_pointers {
            self.compact_pointer[*level as usize] = key.clone();
        }
        Ok(self.current.clone())
    }

    /// 某层的 compaction 轮转起点（user_key 字节）。
    pub fn compact_pointer(&self, level: usize) -> &[u8] {
        &self.compact_pointer[level]
    }

    /// 更新某层的轮转起点。compaction 完成后调用，记录本次 compact 到的最大 user_key。
    pub fn set_compact_pointer(&mut self, level: usize, key: Vec<u8>) {
        self.compact_pointer[level] = key;
    }

    /// 注册一个快照：把指定 seq 加入活跃列表。
    /// seq 由调用方提供（DB 传 MemTable 的最新 seq，而非 last_sequence——后者只在 flush 时同步）。
    /// 调用方需保证持锁调用（快照注册是 VersionSet 状态变更）。
    pub fn acquire_snapshot(&mut self, seq: u64) {
        self.snapshots.push(seq);
    }

    /// 释放快照：从活跃列表移除该 seq。调用方需保证持锁。
    /// 移除后该快照引用的旧版本可能在下次 compaction 中被回收。
    pub fn release_snapshot(&mut self, seq: u64) {
        self.snapshots.retain(|&s| s != seq);
    }

    /// 最老活跃快照的 seq。compaction 丢弃旧版本时的下限：
    /// seq > 此值的版本可能被快照引用，必须保留。
    /// 无活跃快照时返回 MAX_SEQUENCE（任何旧版本都可被回收）。
    pub fn oldest_snapshot_seq(&self) -> u64 {
        self.snapshots.iter().copied().min().unwrap_or(MAX_SEQUENCE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_meta::FileMetaData;
    use crate::internal_key::{InternalKey, ValueType};
    use crate::manifest::write_current;
    use std::path::PathBuf;

    fn meta(num: u64, size: u64, a: &[u8], b: &[u8]) -> FileMetaData {
        FileMetaData::new(
            FileNumber(num),
            size,
            InternalKey::new(a.to_vec(), 1, ValueType::Put),
            InternalKey::new(b.to_vec(), 2, ValueType::Put),
        )
    }

    fn tmp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "mulan-version-test-{}-{}-{}",
            std::process::id(),
            label,
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn empty_version_has_no_files() {
        let v = Version::empty();
        for level in 0..NUM_LEVELS {
            assert_eq!(v.num_files(level), 0);
            assert_eq!(v.total_size(level), 0);
        }
    }

    #[test]
    fn apply_edit_adds_and_removes_files() {
        let base = Version::empty();
        let mut e1 = VersionEdit::new();
        e1.add_file(0, meta(1, 100, b"a", b"z"))
            .add_file(0, meta(2, 200, b"m", b"y"));
        let v1 = apply_edit(&base, &e1).unwrap();
        assert_eq!(v1.num_files(0), 2);
        assert_eq!(v1.total_size(0), 300);

        // 删掉 1 号，加一个 3 号到 level 1。
        let mut e2 = VersionEdit::new();
        e2.add_delete(0, FileNumber(1))
            .add_file(1, meta(3, 50, b"c", b"d"));
        let v2 = apply_edit(&v1, &e2).unwrap();
        assert_eq!(v2.num_files(0), 1);
        assert_eq!(v2.files_at(0)[0].number, FileNumber(2));
        assert_eq!(v2.num_files(1), 1);
        assert_eq!(v2.files_at(1)[0].number, FileNumber(3));
    }

    #[test]
    fn apply_edit_rejects_out_of_range_level() {
        let base = Version::empty();
        let mut e = VersionEdit::new();
        e.add_file(NUM_LEVELS as u32, meta(1, 100, b"a", b"z"));
        assert!(apply_edit(&base, &e).is_err());

        let mut e2 = VersionEdit::new();
        e2.add_delete(NUM_LEVELS as u32, FileNumber(1));
        assert!(apply_edit(&base, &e2).is_err());
    }

    #[test]
    fn delete_nonexistent_file_is_noop() {
        let base = Version::empty();
        let mut e = VersionEdit::new();
        e.add_delete(0, FileNumber(999));
        let v = apply_edit(&base, &e).unwrap();
        assert_eq!(v.num_files(0), 0);
    }

    #[test]
    fn arc_reference_keeps_old_version_alive() {
        // 取当前 Arc，写新 version 后旧 Arc 仍指向旧 Version（不变）。
        let dir = tmp_dir("arc");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let v0 = vs.current();
        assert_eq!(v0.num_files(0), 0);

        let mut e = VersionEdit::new();
        e.add_file(0, meta(1, 100, b"a", b"z"));
        let v1 = vs.write_new_version(&e).unwrap();

        assert_eq!(v1.num_files(0), 1);
        // 旧 Arc 不受影响。
        assert_eq!(v0.num_files(0), 0);
        assert!(!Arc::ptr_eq(&v0, &v1));
    }

    #[test]
    fn write_new_version_updates_scalar_metadata() {
        let dir = tmp_dir("scalars");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e = VersionEdit::new();
        e.set_log_number(7)
            .set_next_file_number(10)
            .set_last_sequence(42);
        vs.write_new_version(&e).unwrap();
        assert_eq!(vs.log_number, 7);
        assert_eq!(vs.next_file_number, 10);
        assert_eq!(vs.last_sequence, 42);
    }

    #[test]
    fn recover_replays_edits_to_same_state() {
        let dir = tmp_dir("recover");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e1 = VersionEdit::new();
        e1.set_log_number(10)
            .set_next_file_number(5)
            .set_last_sequence(5);
        vs.write_new_version(&e1).unwrap();

        let mut e2 = VersionEdit::new();
        e2.add_file(0, meta(2, 4096, b"a", b"z"))
            .add_file(0, meta(3, 8192, b"m", b"y"))
            .set_last_sequence(20);
        vs.write_new_version(&e2).unwrap();

        let mut e3 = VersionEdit::new();
        e3.add_delete(0, FileNumber(2)).set_last_sequence(21);
        vs.write_new_version(&e3).unwrap();

        write_current(&dir, FileNumber(1)).unwrap();
        drop(vs);

        let recovered = VersionSet::recover(&dir).unwrap();
        assert_eq!(recovered.log_number, 10);
        assert_eq!(recovered.next_file_number, 5);
        assert_eq!(recovered.last_sequence, 21);
        assert_eq!(recovered.manifest_number, FileNumber(1));
        // 2 号被删，剩 3 号。
        assert_eq!(recovered.current().num_files(0), 1);
        assert_eq!(recovered.current().files_at(0)[0].number, FileNumber(3));
    }

    #[test]
    fn recover_can_continue_appending_after_recover() {
        // recover 后 writer 可继续追加新 edit。
        let dir = tmp_dir("continue");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e1 = VersionEdit::new();
        e1.set_next_file_number(5).set_last_sequence(1);
        vs.write_new_version(&e1).unwrap();
        write_current(&dir, FileNumber(1)).unwrap();
        drop(vs);

        let mut recovered = VersionSet::recover(&dir).unwrap();
        let mut e2 = VersionEdit::new();
        e2.add_file(0, meta(5, 100, b"a", b"b"))
            .set_last_sequence(2);
        recovered.write_new_version(&e2).unwrap();

        // 再 recover 一次，应看到 e2 的文件。
        drop(recovered);
        let final_vs = VersionSet::recover(&dir).unwrap();
        assert_eq!(final_vs.current().num_files(0), 1);
        assert_eq!(final_vs.last_sequence, 2);
    }

    #[test]
    fn snapshot_acquire_returns_current_sequence() {
        // 注册快照后，oldest 应反映该 seq。
        let dir = tmp_dir("snap-acquire");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e = VersionEdit::new();
        e.set_next_file_number(2).set_last_sequence(42);
        vs.write_new_version(&e).unwrap();
        vs.acquire_snapshot(42);
        assert_eq!(vs.oldest_snapshot_seq(), 42);
    }

    #[test]
    fn snapshot_oldest_is_min_when_multiple() {
        // 多个快照时，oldest = 最小 seq。
        let dir = tmp_dir("snap-oldest");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e = VersionEdit::new();
        e.set_next_file_number(2).set_last_sequence(10);
        vs.write_new_version(&e).unwrap();
        let s1 = 10u64;
        vs.acquire_snapshot(s1);
        // 推进 last_sequence 到 20，再注册一个 seq=20 的快照。
        let mut e2 = VersionEdit::new();
        e2.set_last_sequence(20);
        vs.write_new_version(&e2).unwrap();
        let s2 = 20u64;
        vs.acquire_snapshot(s2);
        // oldest 应是较小的 10。
        assert_eq!(vs.oldest_snapshot_seq(), 10);
        // 释放 s1 后 oldest 变成 20。
        vs.release_snapshot(s1);
        assert_eq!(vs.oldest_snapshot_seq(), 20);
        // 释放 s2 后无活跃快照，返回 MAX_SEQUENCE（任何旧版本可回收）。
        vs.release_snapshot(s2);
        assert_eq!(vs.oldest_snapshot_seq(), MAX_SEQUENCE);
    }

    #[test]
    fn snapshot_no_active_returns_max() {
        // 无活跃快照时 oldest = MAX_SEQUENCE。
        let dir = tmp_dir("snap-none");
        let vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        assert_eq!(vs.oldest_snapshot_seq(), MAX_SEQUENCE);
    }

    #[test]
    fn snapshot_release_is_idempotent_safe() {
        // release 一个不存在的 seq 不应 panic（容错）。
        let dir = tmp_dir("snap-release");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e = VersionEdit::new();
        e.set_next_file_number(2).set_last_sequence(5);
        vs.write_new_version(&e).unwrap();
        let s = 5u64;
        vs.acquire_snapshot(s);
        vs.release_snapshot(s);
        vs.release_snapshot(s); // 重复 release，不应 panic
        assert_eq!(vs.oldest_snapshot_seq(), MAX_SEQUENCE);
    }

    /// 验证 compact_pointer 在 recover 后被正确恢复（不再重置为空）。
    #[test]
    fn recover_restores_compact_pointer() {
        let dir = tmp_dir("cp-recover");
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut e1 = VersionEdit::new();
        e1.set_next_file_number(5)
            .set_last_sequence(1)
            .set_compact_pointer(2, b"cp-key".to_vec())
            .set_compact_pointer(4, b"cp-other".to_vec());
        vs.write_new_version(&e1).unwrap();
        write_current(&dir, FileNumber(1)).unwrap();
        drop(vs);

        let recovered = VersionSet::recover(&dir).unwrap();
        assert_eq!(recovered.compact_pointer(2), b"cp-key");
        assert_eq!(recovered.compact_pointer(4), b"cp-other");
        // 未设置的层应为空
        assert!(recovered.compact_pointer(0).is_empty());
    }
}
