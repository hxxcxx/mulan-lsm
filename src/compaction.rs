//! Compaction：后台归并的触发、选文件、执行。

use crate::error::{MulanError, Result};
use crate::file_meta::{sst_path, FileMetaData, FileNumber, IdGenerator};
use crate::internal_key::{
    user_key_of_internal_key, vtype_of_internal_key, InternalKey, ValueType,
};
use crate::iterator::{LsmIterator, MergingIterator};
use crate::sstable::table::{TableBuilder, TableReader};
use crate::version::{Version, VersionSet, NUM_LEVELS};

/// L0 文件数达到此值触发 compaction。LevelDB 默认 4。
pub const L0_COMPACTION_TRIGGER: usize = 4;

/// L0 文件数达到此值时前台写阻塞，等待 compaction 降下来。LevelDB 默认 12。
pub const LEVEL0_STOP_WRITES_TRIGGER: usize = 12;

/// 单个 SSTable 目标大小。LevelDB 默认 2MB。
pub const TARGET_FILE_SIZE: u64 = 2 * 1024 * 1024;

/// L1 的目标大小基数。LevelDB 默认 10MB。
pub const MAX_BYTES_FOR_LEVEL_BASE: u64 = 10 * 1024 * 1024;

/// 每层目标大小是上一层的多少倍。LevelDB 默认 10。
pub const MAX_BYTES_FOR_LEVEL_MULTIPLIER: u64 = 10;

/// 计算某层的目标最大字节数。
/// L0 不按大小算（按文件数），这里给 L1+ 用：base * multiplier^(level-1)。
pub fn max_bytes_for_level(level: usize) -> u64 {
    if level <= 1 {
        MAX_BYTES_FOR_LEVEL_BASE
    } else {
        MAX_BYTES_FOR_LEVEL_BASE * MAX_BYTES_FOR_LEVEL_MULTIPLIER.pow((level - 1) as u32)
    }
}

/// 计算某层的 compaction score。
/// - L0：文件数 / `L0_COMPACTION_TRIGGER`（L0 文件区间重叠，读放大与文件数正相关）
/// - L1+：层总大小 / `max_bytes_for_level(L)`
///
/// score > 1.0 表示该层"过载"需要 compact。返回 (level, score) 中 score 最大的。
pub fn compaction_score(version: &Version) -> Option<(usize, f64)> {
    let mut best: Option<(usize, f64)> = None;
    for level in 0..NUM_LEVELS {
        let score = if level == 0 {
            version.num_files(0) as f64 / L0_COMPACTION_TRIGGER as f64
        } else {
            let size = version.total_size(level);
            // 空层 score=0，不参与。
            if size == 0 {
                continue;
            }
            size as f64 / max_bytes_for_level(level) as f64
        };
        if score > 0.0 && best.is_none_or(|(_, bs)| score > bs) {
            best = Some((level, score));
        }
    }
    best
}

/// 判断是否需要因 L0 文件过多而阻塞前台写。
pub fn level0_needs_slowdown(version: &Version) -> bool {
    version.num_files(0) >= LEVEL0_STOP_WRITES_TRIGGER
}

/// 一次 compaction 的输入集合。
/// `inputs[0]` 是本层输入，`inputs[1]` 是下一层（level+1）输入。
/// L0 compaction 时 inputs[0] 含多个 L0 文件（区间重叠故全选）；
/// L1+ compaction 时 inputs[0] 通常只有 1 个文件。
pub struct Compaction {
    pub level: usize,
    pub inputs: [Vec<FileMetaData>; 2],
    /// 祖父层（level+2）中与本 compaction 区间重叠的文件。
    /// 用于输出切分控制：与祖父层重叠累计大小超阈值时切新输出文件，
    /// 防止单次 compaction 输出与祖父层过度重叠（避免下次 compaction 输入过多）。
    pub grandparents: Vec<FileMetaData>,
}

impl Compaction {
    /// 本 compaction 涉及的 user_key 区间（inputs[0] + inputs[1] 的并集）。
    pub fn smallest_user_key(&self) -> &[u8] {
        // inputs[0] 非空时取其首文件 smallest；否则取 inputs[1] 首。
        if let Some(f) = self.inputs[0].first() {
            &f.smallest.user_key
        } else if let Some(f) = self.inputs[1].first() {
            &f.smallest.user_key
        } else {
            &[]
        }
    }

    pub fn largest_user_key(&self) -> &[u8] {
        if let Some(f) = self.inputs[0].last() {
            &f.largest.user_key
        } else if let Some(f) = self.inputs[1].last() {
            &f.largest.user_key
        } else {
            &[]
        }
    }

    /// 判断某 user_key 是否在 level+2 及更深层都不出现（即本 compaction 是该 key 的"基底层"）。
    ///
    /// 删除标记只有在基底层才能丢弃：若更高层仍有该 key 的旧版本，丢删除标记会让旧版本"复活"。
    pub fn is_base_level_for_user_key(&self, version: &Version, user_key: &[u8]) -> bool {
        for level in (self.level + 2)..NUM_LEVELS {
            if !version.get_overlaps(level, user_key, user_key).is_empty() {
                return false;
            }
        }
        true
    }

    /// 所有输入文件的编号集合（用于提交时构造 deleted_files）。
    pub fn input_file_numbers(&self) -> Vec<(usize, FileNumber)> {
        let mut nums = Vec::new();
        for f in &self.inputs[0] {
            nums.push((self.level, f.number));
        }
        for f in &self.inputs[1] {
            nums.push((self.level + 1, f.number));
        }
        nums
    }
}

/// 根据 compaction score 和轮转起点选出一次 compaction 的输入文件。
///
/// - L0：文件区间重叠，选所有与"compact_pointer 之后"重叠的 L0 文件；
///   若 pointer 为空则选全部 L0。然后选 L1 中与之重叠的文件。
/// - L1+：从 compact_pointer 之后选第一个文件，再选 level+1 中与该文件重叠的文件。
pub fn pick_compaction(vs: &VersionSet) -> Option<Compaction> {
    let version = vs.current();
    let (level, _score) = compaction_score(&version)?;
    if level >= NUM_LEVELS - 1 {
        // 最后一层不 compact（没有下一层可归并）。
        return None;
    }

    let mut inputs: [Vec<FileMetaData>; 2] = [Vec::new(), Vec::new()];

    if level == 0 {
        // L0：选全部 L0 文件（它们区间可能重叠，归并时必须全选）。
        // LevelDB 实际选"与 compact_pointer 重叠的"，但简化为全选——L0 文件数本身受 trigger 控制。
        inputs[0] = version.files_at(0).to_vec();
    } else {
        // L1+：从 compact_pointer 之后选第一个文件。
        let pointer = vs.compact_pointer(level);
        let mut picked: Option<FileMetaData> = None;
        for f in version.files_at(level) {
            if pointer.is_empty() || f.largest.user_key.as_slice() > pointer {
                picked = Some(f.clone());
                break;
            }
        }
        let picked = picked?;
        inputs[0] = vec![picked];
    }

    // 选 level+1 中与本层输入区间重叠的文件。
    if !inputs[0].is_empty() {
        // 对 L0，inputs[0] 可能多个文件区间不连续，用并集区间 [最小 smallest, 最大 largest]。
        let range_smallest = inputs[0]
            .iter()
            .map(|f| f.smallest.user_key.as_slice())
            .min()
            .unwrap()
            .to_vec();
        let range_largest = inputs[0]
            .iter()
            .map(|f| f.largest.user_key.as_slice())
            .max()
            .unwrap()
            .to_vec();
        inputs[1] = version.get_overlaps(level + 1, &range_smallest, &range_largest);
    }

    // 计算祖父层重叠（供输出切分控制用）。取所有输入文件（本层+下一层）的并集区间。
    let grandparents = if level + 2 < NUM_LEVELS && !inputs[0].is_empty() {
        let all_files: Vec<&FileMetaData> = inputs.iter().flat_map(|v| v.iter()).collect();
        let gp_smallest = all_files
            .iter()
            .map(|f| f.smallest.user_key.as_slice())
            .min()
            .unwrap();
        let gp_largest = all_files
            .iter()
            .map(|f| f.largest.user_key.as_slice())
            .max()
            .unwrap();
        version.get_overlaps(level + 2, gp_smallest, gp_largest)
    } else {
        Vec::new()
    };

    Some(Compaction {
        level,
        inputs,
        grandparents,
    })
}

/// 祖父层重叠超过此阈值时切分输出文件（防下次 compaction 一次捞太多）。
/// LevelDB: 10 * target_file_size。
const MAX_GRANDPARENT_OVERLAP_BYTES: u64 = 10 * TARGET_FILE_SIZE;

/// 归并执行结果：新生成的文件（要加入 level+1）+ 被替换的旧文件编号（要从原层删除）。
pub struct CompactionOutput {
    /// 新文件落到 level+1 层。
    pub new_files: Vec<FileMetaData>,
    /// 要删除的旧文件 (level, number)。本层和下一层的输入文件都要删。
    pub deleted_files: Vec<(usize, FileNumber)>,
}

/// 执行一次 compaction：归并输入文件，写新 SSTable，丢弃旧版本和可丢删除标记。
///
/// 输出文件按 `TARGET_FILE_SIZE` 或与祖父层重叠超 `MAX_GRANDPARENT_OVERLAP_BYTES` 切分。
/// 删除标记丢弃规则：最新版本是 Delete 且 `is_base_level_for_user_key` 为真 → 不写入；
/// 否则写入（保留删除标记供更高层归并时判断）。
///
/// `id_gen` 由调用方提供，用于分配输出文件编号。
pub fn do_compaction(
    dir: &std::path::Path,
    compaction: &Compaction,
    version: &Version,
    id_gen: &mut IdGenerator,
    oldest_snapshot_seq: u64,
    block_target: usize,
    bits_per_key: usize,
) -> Result<CompactionOutput> {
    // 1. 为每个输入文件开 TableIter，直接作为 LsmIterator 喂给归并（惰性，不全量 collect）。
    let mut iters: Vec<Box<dyn LsmIterator>> = Vec::new();
    for level_files in &compaction.inputs {
        for f in level_files {
            let reader = TableReader::open(&sst_path(dir, f.number))?;
            iters.push(Box::new(reader.into_table_iter()?));
        }
    }
    let mut merger = MergingIterator::new(iters);

    // 2. 归并写新 SSTable，按阈值切分。
    let mut new_files: Vec<FileMetaData> = Vec::new();
    let mut current_builder: Option<TableBuilder> = None;
    let mut current_smallest: Option<InternalKey> = None;
    let mut current_largest: Option<InternalKey> = None;
    let mut current_grandparent_overlap: u64 = 0;
    let mut current_file_number: Option<FileNumber> = None;
    // 祖父文件游标：已累加过重叠到第几个祖父文件。grandparents 按 smallest 有序，
    // user_key 全局单调推进，故游标只前进不回退——保证每个祖父文件至多计入一次重叠。
    let mut grandparent_idx: usize = 0;
    // 上一条已处理 entry 的 user_key + seq，用于版本丢弃判定（见 drop_entry）。
    let mut last_user_key: Option<Vec<u8>> = None;
    let mut last_seq: u64 = 0;

    while let Some((ik_bytes, value)) = merger.next() {
        let vtype = vtype_of_internal_key(&ik_bytes);
        let user_key = user_key_of_internal_key(&ik_bytes);
        let ik = InternalKey::decode(&ik_bytes)?;
        let seq = ik.seq;

        // 版本丢弃判定（LevelDB 语义）：
        // - 与上一条同 user_key 且上一条 seq ≤ oldest_snapshot_seq → 当前条（更旧）可丢，
        //   因为上一条是"快照可见的更新版本"，当前条对任何快照都无意义。
        // - 新 user_key 的最新版本：若 Delete 且 is_base_level 且 seq ≤ oldest_snapshot_seq → 可丢，
        //   否则保留（含 Delete——若 seq > oldest，快照之后可能有更新写入，删了会让旧版本复活）。
        let is_same_key_as_last = last_user_key.as_deref() == Some(user_key);
        let last_visible_to_snapshot = last_seq <= oldest_snapshot_seq;
        let delete_droppable = vtype == ValueType::Delete
            && seq <= oldest_snapshot_seq
            && compaction.is_base_level_for_user_key(version, user_key);
        let drop = (is_same_key_as_last && last_visible_to_snapshot) || delete_droppable;
        last_user_key = Some(user_key.to_vec());
        last_seq = seq;
        if drop {
            continue;
        }

        // 若尚无输出文件，开一个并写入本条。
        if current_builder.is_none() {
            let num = id_gen.new_file_number();
            current_file_number = Some(num);
            let file = std::fs::File::create(sst_path(dir, num))?;
            current_builder = Some(TableBuilder::with_options(file, block_target, bits_per_key));
            current_smallest = Some(ik.clone());
            current_largest = Some(ik.clone());
            current_grandparent_overlap = 0;
            continue_with_entry(
                &mut current_builder,
                user_key,
                &ik_bytes,
                &value,
                &mut current_largest,
            )?;
            continue;
        }

        // 检查是否需要切分当前输出文件。
        let builder = current_builder.as_ref().unwrap();
        let should_split = builder.current_size_estimate() >= TARGET_FILE_SIZE
            || current_grandparent_overlap > MAX_GRANDPARENT_OVERLAP_BYTES;

        if should_split {
            // 切出当前文件，开新文件。
            let meta = finish_current(
                dir,
                &mut current_builder,
                current_file_number,
                &current_smallest,
                &current_largest,
            )?;
            new_files.push(meta);
            let num = id_gen.new_file_number();
            current_file_number = Some(num);
            let file = std::fs::File::create(sst_path(dir, num))?;
            current_builder = Some(TableBuilder::with_options(file, block_target, bits_per_key));
            current_smallest = Some(ik.clone());
            current_largest = Some(ik.clone());
            current_grandparent_overlap = 0;
            // 游标归 0：新输出文件从当前 user_key 起，需重新计算它与哪些祖父重叠。
            // 不能保持游标——否则跨越切分点的大祖父文件（如 [a,z] 同时覆盖前后两个文件）
            // 只会被前一个文件计入，后一个文件永久漏算，导致后一个文件 overlap 恒为 0、
            // 永不因祖父重叠切分（漏算）。归 0 后下面的推进循环会重新计入覆盖新起点的祖父。
            // 每个输出文件独立计算自己与祖父的重叠：同一祖父若覆盖多个输出文件，会被各计一次
            // （这是对的——每个输出文件确实与该祖父重叠）。区别于原 bug（同一文件内重复计入）。
            grandparent_idx = 0;
        }

        // 累积祖父层重叠：推进游标，跳过完全在 user_key 左侧的祖父，计入覆盖 user_key 的。
        // grandparents 按 smallest 有序、区间不重叠（L2+ 保证）。游标在切分时归 0，
        // 故此循环每条 entry 从当前 gp_idx 扫到第一个未覆盖 user_key 的祖父为止。
        while grandparent_idx < compaction.grandparents.len() {
            let gp = &compaction.grandparents[grandparent_idx];
            if user_key > gp.largest.user_key.as_slice() {
                // 该祖父文件完全在 user_key 左侧，游标前进。
                grandparent_idx += 1;
                continue;
            }
            if user_key < gp.smallest.user_key.as_slice() {
                // 该祖父文件完全在 user_key 右侧，后续都不重叠，停止。
                break;
            }
            // user_key 落在该祖父文件 [smallest, largest] 区间内：计入一次。
            current_grandparent_overlap += gp.file_size;
            grandparent_idx += 1;
        }

        continue_with_entry(
            &mut current_builder,
            user_key,
            &ik_bytes,
            &value,
            &mut current_largest,
        )?;
    }

    // 收尾：最后一个未 finish 的 builder。
    if let Some(_builder) = current_builder.as_ref() {
        let meta = finish_current(
            dir,
            &mut current_builder,
            current_file_number,
            &current_smallest,
            &current_largest,
        )?;
        new_files.push(meta);
    }

    // 3. 被替换的旧文件 = 所有输入文件。
    let mut deleted_files = Vec::new();
    for f in &compaction.inputs[0] {
        deleted_files.push((compaction.level, f.number));
    }
    for f in &compaction.inputs[1] {
        deleted_files.push((compaction.level + 1, f.number));
    }

    Ok(CompactionOutput {
        new_files,
        deleted_files,
    })
}

/// 把一条 entry 写入当前 builder，更新 largest。
fn continue_with_entry(
    builder: &mut Option<TableBuilder>,
    user_key: &[u8],
    ik_bytes: &[u8],
    value: &[u8],
    current_largest: &mut Option<InternalKey>,
) -> Result<()> {
    let b = builder
        .as_mut()
        .ok_or_else(|| MulanError::Corrupted("compaction builder missing".into()))?;
    b.add(user_key, ik_bytes, value)?;
    *current_largest = Some(InternalKey::decode(ik_bytes)?);
    Ok(())
}

/// finish 当前 builder，构造 FileMetaData，返回。
fn finish_current(
    dir: &std::path::Path,
    builder: &mut Option<TableBuilder>,
    number: Option<FileNumber>,
    smallest: &Option<InternalKey>,
    largest: &Option<InternalKey>,
) -> Result<FileMetaData> {
    let b = builder
        .take()
        .ok_or_else(|| MulanError::Corrupted("compaction builder missing at finish".into()))?;
    b.finish()?;
    let num = number
        .ok_or_else(|| MulanError::Corrupted("compaction file number missing".into()))?;
    let file_size = std::fs::metadata(sst_path(dir, num))?.len();
    Ok(FileMetaData::new(
        num,
        file_size,
        smallest
            .clone()
            .ok_or_else(|| MulanError::Corrupted("compaction smallest key missing".into()))?,
        largest
            .clone()
            .ok_or_else(|| MulanError::Corrupted("compaction largest key missing".into()))?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_meta::{FileMetaData, FileNumber};
    use crate::internal_key::{InternalKey, ValueType};
    use crate::version::VersionSet;

    fn meta(num: u64, size: u64, a: &[u8], b: &[u8]) -> FileMetaData {
        FileMetaData::new(
            FileNumber(num),
            size,
            InternalKey::new(a.to_vec(), 1, ValueType::Put),
            InternalKey::new(b.to_vec(), 2, ValueType::Put),
        )
    }

    fn version_with_files(level: usize, files: Vec<FileMetaData>) -> Version {
        // Version 的 files 字段私有，用 VersionSet::write_new_version 间接构造。
        let dir = std::env::temp_dir().join(format!(
            "mulan-compaction-score-test-{}-{}",
            std::process::id(),
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut edit = crate::manifest::VersionEdit::new();
        for f in files {
            edit.add_file(level as u32, f);
        }
        vs.write_new_version(&edit).unwrap();
        (*vs.current()).clone()
    }

    #[test]
    fn empty_version_has_no_compaction_score() {
        let v = Version::empty();
        assert!(compaction_score(&v).is_none());
    }

    #[test]
    fn l0_score_by_file_count() {
        // 5 个 L0 文件 → score = 5/4 = 1.25。
        let files: Vec<_> = (0..5)
            .map(|i| meta(i as u64 + 1, 1000, b"a", b"z"))
            .collect();
        let v = version_with_files(0, files);
        let (level, score) = compaction_score(&v).unwrap();
        assert_eq!(level, 0);
        assert!((score - 1.25).abs() < 0.001);
    }

    #[test]
    fn l0_below_trigger_no_score() {
        // 3 个 L0 文件 → score = 3/4 = 0.75 < 1，但 score > 0 仍会被选中。
        // compaction_score 返回 score 最大的层，只要 > 0 就返回。
        let files: Vec<_> = (0..3)
            .map(|i| meta(i as u64 + 1, 1000, b"a", b"z"))
            .collect();
        let v = version_with_files(0, files);
        let (level, _) = compaction_score(&v).unwrap();
        assert_eq!(level, 0);
    }

    #[test]
    fn l1_score_by_size() {
        // L1 总大小 15MB → score = 15/10 = 1.5（L0 无文件时选 L1）。
        let files = vec![meta(1, 15 * 1024 * 1024, b"a", b"z")];
        let v = version_with_files(1, files);
        let (level, score) = compaction_score(&v).unwrap();
        assert_eq!(level, 1);
        assert!((score - 1.5).abs() < 0.001);
    }

    #[test]
    fn picks_highest_score_level() {
        // L0 有 3 文件（score=0.75），L1 有 15MB（score=1.5）→ 选 L1。
        let dir = std::env::temp_dir().join(format!(
            "mulan-compaction-pick-{}-{}",
            std::process::id(),
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut edit = crate::manifest::VersionEdit::new();
        for i in 0..3u64 {
            edit.add_file(0, meta(i + 1, 1000, b"a", b"z"));
        }
        edit.add_file(1, meta(10, 15 * 1024 * 1024, b"a", b"z"));
        vs.write_new_version(&edit).unwrap();
        let v = (*vs.current()).clone();
        let (level, _) = compaction_score(&v).unwrap();
        assert_eq!(level, 1, "L1 score 1.5 > L0 score 0.75");
    }

    #[test]
    fn max_bytes_for_level_grows_tenfold() {
        assert_eq!(max_bytes_for_level(1), MAX_BYTES_FOR_LEVEL_BASE);
        assert_eq!(max_bytes_for_level(2), MAX_BYTES_FOR_LEVEL_BASE * 10);
        assert_eq!(max_bytes_for_level(3), MAX_BYTES_FOR_LEVEL_BASE * 100);
    }

    #[test]
    fn level0_slowdown_trigger() {
        let files: Vec<_> = (0..LEVEL0_STOP_WRITES_TRIGGER)
            .map(|i| meta(i as u64 + 1, 1000, b"a", b"z"))
            .collect();
        let v = version_with_files(0, files);
        assert!(level0_needs_slowdown(&v));
    }

    /// 构造一个含多层的 VersionSet（测试 pick_compaction 用）。
    fn vs_with_files(files_by_level: &[(usize, FileMetaData)]) -> (VersionSet, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "mulan-pick-test-{}-{}",
            std::process::id(),
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let mut vs = VersionSet::new_pending(&dir, FileNumber(1)).unwrap();
        let mut edit = crate::manifest::VersionEdit::new();
        for (level, f) in files_by_level {
            edit.add_file(*level as u32, f.clone());
        }
        vs.write_new_version(&edit).unwrap();
        (vs, dir)
    }

    #[test]
    fn pick_l0_selects_all_l0_files() {
        // L0 有 5 个重叠文件 → pick 选全部 5 个 L0 + L1 重叠文件。
        let l0_files: Vec<_> = (0..5)
            .map(|i| meta(i as u64 + 1, 1000, b"a", b"z"))
            .collect();
        let l1_file = meta(10, 1000, b"a", b"z");
        let mut all = l0_files.iter().map(|f| (0, f.clone())).collect::<Vec<_>>();
        all.push((1, l1_file));
        let (vs, _dir) = vs_with_files(&all);
        let compaction = pick_compaction(&vs).unwrap();
        assert_eq!(compaction.level, 0);
        assert_eq!(compaction.inputs[0].len(), 5, "L0 全选");
        assert_eq!(compaction.inputs[1].len(), 1, "L1 重叠选 1 个");
    }

    #[test]
    fn pick_l1_selects_one_file_after_pointer() {
        // L1 有 5 个不重叠文件，pointer 为空 → 选第一个 + L2 重叠。
        let l1_files: Vec<_> = vec![
            meta(1, 100, b"a", b"c"),
            meta(2, 100, b"d", b"f"),
            meta(3, 100, b"g", b"i"),
            meta(4, 100, b"j", b"l"),
            meta(5, 100, b"m", b"o"),
        ];
        let l2_file = meta(20, 100, b"a", b"c");
        let mut all = l1_files.iter().map(|f| (1, f.clone())).collect::<Vec<_>>();
        all.push((2, l2_file));
        let (vs, _dir) = vs_with_files(&all);
        let compaction = pick_compaction(&vs).unwrap();
        assert_eq!(compaction.level, 1);
        assert_eq!(compaction.inputs[0].len(), 1, "L1 选 1 个文件");
        assert_eq!(compaction.inputs[0][0].number, FileNumber(1));
        assert_eq!(compaction.inputs[1].len(), 1, "L2 重叠选 1 个");
    }

    #[test]
    fn pick_l1_respects_compact_pointer() {
        // pointer 设到 "c" 之后 → 跳过第一个文件（a-c），从 d-f 开始选。
        let l1_files: Vec<_> = vec![
            meta(1, 100, b"a", b"c"),
            meta(2, 100, b"d", b"f"),
            meta(3, 100, b"g", b"i"),
        ];
        let all: Vec<_> = l1_files.iter().map(|f| (1, f.clone())).collect();
        let (mut vs, _dir) = vs_with_files(&all);
        // 设 pointer 为 "c"：第一个文件 largest="c" 不 > "c"，跳过；选第二个。
        vs.set_compact_pointer(1, b"c".to_vec());
        let compaction = pick_compaction(&vs).unwrap();
        assert_eq!(compaction.inputs[0][0].number, FileNumber(2));
    }

    #[test]
    fn is_base_level_true_when_no_higher_overlap() {
        // L1 compaction，key "k" 只在 L1 出现，L2+ 无 → is_base = true。
        let l1_file = meta(1, 100, b"k", b"k");
        let (vs, _dir) = vs_with_files(&[(1, l1_file)]);
        let compaction = pick_compaction(&vs).unwrap();
        assert!(compaction.is_base_level_for_user_key(&vs.current(), b"k"));
    }

    #[test]
    fn is_base_level_false_when_higher_layer_has_key() {
        // L1 compaction，key "k" 在 L3 也出现 → is_base = false。
        let l1_file = meta(1, 100, b"k", b"k");
        let l3_file = meta(3, 100, b"k", b"k");
        let (vs, _dir) = vs_with_files(&[(1, l1_file), (3, l3_file)]);
        let compaction = pick_compaction(&vs).unwrap();
        assert!(!compaction.is_base_level_for_user_key(&vs.current(), b"k"));
    }

    #[test]
    fn is_base_level_checks_levels_above_plus_two() {
        // level=1 → 检查 level 3,4,5,6。L2 有 key 不影响 is_base（L2 是下一层，归并会处理）。
        let l1_file = meta(1, 100, b"k", b"k");
        let l2_file = meta(2, 100, b"k", b"k"); // 下一层，不影响 is_base
        let (vs, _dir) = vs_with_files(&[(1, l1_file), (2, l2_file)]);
        let compaction = pick_compaction(&vs).unwrap();
        assert!(compaction.is_base_level_for_user_key(&vs.current(), b"k"));
    }

    #[test]
    fn get_overlaps_returns_intersecting_files() {
        // L2 有 3 个不重叠文件，查 [d, f] 应只返回中间那个。
        let files = vec![
            meta(1, 100, b"a", b"c"),
            meta(2, 100, b"d", b"f"),
            meta(3, 100, b"g", b"i"),
        ];
        let v = version_with_files(2, files);
        let overlaps = v.get_overlaps(2, b"d", b"f");
        assert_eq!(overlaps.len(), 1);
        assert_eq!(overlaps[0].number, FileNumber(2));
    }

    #[test]
    fn get_overlaps_boundary_keys() {
        // 查 [c, g] 跨 3 个文件的边界。
        let files = vec![
            meta(1, 100, b"a", b"c"),
            meta(2, 100, b"d", b"f"),
            meta(3, 100, b"g", b"i"),
        ];
        let v = version_with_files(2, files);
        let overlaps = v.get_overlaps(2, b"c", b"g");
        // a-c 与 c-g 重叠（c 是边界），d-f 重叠，g-i 重叠（g 是边界）。
        assert_eq!(overlaps.len(), 3);
    }

    #[test]
    fn input_file_numbers_covers_both_inputs() {
        let l0 = meta(1, 100, b"a", b"z");
        let l1 = meta(2, 100, b"a", b"z");
        let (vs, _dir) = vs_with_files(&[(0, l0), (1, l1)]);
        let compaction = pick_compaction(&vs).unwrap();
        let nums = compaction.input_file_numbers();
        assert!(nums.contains(&(0, FileNumber(1))));
        assert!(nums.contains(&(1, FileNumber(2))));
    }

    use crate::file_meta::IdGenerator;
    use crate::memtable::MemTable;
    use crate::sstable::table::TableReader;

    /// 用 MemTable 生成一个 SSTable 文件，返回 FileMetaData。
    /// `start_seq` 让调用方控制起始 seq，避免多文件间 seq 重叠（破坏去重语义）。
    fn build_sst(
        dir: &std::path::Path,
        num: FileNumber,
        start_seq: u64,
        kvs: &[(Vec<u8>, Vec<u8>)],
    ) -> FileMetaData {
        let mut mem = MemTable::with_initial_sequence(start_seq);
        for (k, v) in kvs {
            mem.put(k, v);
        }
        let path = crate::file_meta::sst_path(dir, num);
        let result = mem.flush_to_sstable_with_bounds(&path).unwrap();
        let size = std::fs::metadata(&path).unwrap().len();
        FileMetaData::new(num, size, result.smallest.unwrap(), result.largest.unwrap())
    }

    fn tmp_compaction_dir(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mulan-compaction-do-{}-{}-{}",
            std::process::id(),
            label,
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn compaction_merges_two_overlapping_files() {
        // 两个 L0 文件有重叠 key，归并后旧版本消失，只剩最新。
        let dir = tmp_compaction_dir("merge2");
        let f1 = build_sst(
            &dir,
            FileNumber(1),
            0,
            &[
                (b"a".to_vec(), b"a1".to_vec()),
                (b"b".to_vec(), b"b1".to_vec()),
            ],
        );
        let f2 = build_sst(
            &dir,
            FileNumber(2),
            100,
            &[
                (b"a".to_vec(), b"a2".to_vec()),
                (b"c".to_vec(), b"c2".to_vec()),
            ],
        );
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1, f2], vec![]],
            grandparents: vec![],
        };
        let mut id_gen = IdGenerator::new(100);
        let version = Version::empty();
        let output = do_compaction(
            &dir,
            &compaction,
            &version,
            &mut id_gen,
            crate::internal_key::MAX_SEQUENCE,
            4 * 1024,
            10,
        )
        .unwrap();
        assert_eq!(output.new_files.len(), 1);
        assert_eq!(output.deleted_files.len(), 2);

        // 读输出文件，验证 a 的最新版本是 a2。
        let reader = TableReader::open(&crate::file_meta::sst_path(
            &dir,
            output.new_files[0].number,
        ))
        .unwrap();
        assert_eq!(reader.get(b"a").unwrap(), Some(b"a2".as_slice()));
        assert_eq!(reader.get(b"b").unwrap(), Some(b"b1".as_slice()));
        assert_eq!(reader.get(b"c").unwrap(), Some(b"c2".as_slice()));
    }

    #[test]
    fn compaction_splits_output_by_target_size() {
        // 写大量数据让输出超 TARGET_FILE_SIZE（2MB），应切出多个文件。
        let dir = tmp_compaction_dir("split");
        let big_val = vec![b'x'; 100_000]; // 100KB value
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..100)
            .map(|i| (format!("key{i:03}").into_bytes(), big_val.clone()))
            .collect();
        let f1 = build_sst(&dir, FileNumber(1), 0, &kvs);
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1], vec![]],
            grandparents: vec![],
        };
        let mut id_gen = IdGenerator::new(100);
        let version = Version::empty();
        let output = do_compaction(
            &dir,
            &compaction,
            &version,
            &mut id_gen,
            crate::internal_key::MAX_SEQUENCE,
            4 * 1024,
            10,
        )
        .unwrap();
        // 100 * 100KB = 10MB，应切出多个 ~2MB 文件。
        assert!(
            output.new_files.len() > 1,
            "should split into multiple files, got {}",
            output.new_files.len()
        );
        // 每个输出文件不超 TARGET_FILE_SIZE 太多（允许最后一个略超）。
        for f in &output.new_files {
            assert!(
                f.file_size < 3 * TARGET_FILE_SIZE,
                "file too big: {}",
                f.file_size
            );
        }
    }

    #[test]
    fn grandparent_overlap_recounted_after_split() {
        // 验证切分后新输出文件重新计算祖父重叠（游标归 0 修复）。
        // 构造一个大祖父文件 [a,z]，file_size=30MB > MAX_GRANDPARENT_OVERLAP_BYTES(20MB)，
        // 它覆盖全部输入区间。输入 6 个 key（a..f），每条 entry 都落入该祖父区间。
        // 修复正确时：第 1 条计入祖父 → overlap=30MB > 20MB → 切分；新文件游标归 0，
        // 下一条又计入 → 又切分……故应产生多个输出文件。
        // 若游标不归 0（漏算 bug）：第 1 个文件切分后，后续文件 overlap 恒 0，
        // 全挤进 1 个文件（靠 size 才切，但这里数据小不会到 size）。
        let dir = tmp_compaction_dir("gp_split");
        let kvs: Vec<(Vec<u8>, Vec<u8>)> = (0..6u32)
            .map(|i| (format!("k{i}").into_bytes(), b"v".to_vec()))
            .collect();
        let f1 = build_sst(&dir, FileNumber(1), 0, &kvs);
        // 祖父文件 [a, z]，file_size 虚构为 30MB（不需要真实文件存在，do_compaction 只读元数据）。
        let grandparent = FileMetaData::new(
            FileNumber(99),
            30 * 1024 * 1024,
            InternalKey::new(b"a".to_vec(), 0, ValueType::Put),
            InternalKey::new(b"z".to_vec(), 0, ValueType::Put),
        );
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1], vec![]],
            grandparents: vec![grandparent],
        };
        let mut id_gen = IdGenerator::new(100);
        let version = Version::empty();
        let output = do_compaction(
            &dir,
            &compaction,
            &version,
            &mut id_gen,
            crate::internal_key::MAX_SEQUENCE,
            4 * 1024,
            10,
        )
        .unwrap();
        // 修复正确：每条 entry 后都因 overlap>20MB 切分，应产生多个文件（>1）。
        // 6 条 key → 至少 2 个文件（第 1 个文件写 1 条后第 2 条触发切分，依此类推）。
        assert!(
            output.new_files.len() > 1,
            "切分后应重新计算祖父重叠产生多个文件，实际 {} 个（可能漏算：游标未归 0）",
            output.new_files.len()
        );
        // 验证全部 key 都写入了（分布在多个文件里）。
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        for f in &output.new_files {
            let reader = TableReader::open(&crate::file_meta::sst_path(&dir, f.number)).unwrap();
            for (ik_bytes, _) in reader.into_table_iter().unwrap() {
                all_keys.push(user_key_of_internal_key(&ik_bytes).to_vec());
            }
        }
        all_keys.sort();
        assert_eq!(all_keys.len(), 6, "全部 6 条 key 都应写入");
    }

    #[test]
    fn compaction_drops_old_versions() {
        let dir = tmp_compaction_dir("dropold");
        let f1 = build_sst(
            &dir,
            FileNumber(1),
            0,
            &[
                (b"k".to_vec(), b"v1".to_vec()),
                (b"k".to_vec(), b"v2".to_vec()),
                (b"k".to_vec(), b"v3".to_vec()),
            ],
        );
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1], vec![]],
            grandparents: vec![],
        };
        let mut id_gen = IdGenerator::new(100);
        let version = Version::empty();
        let output = do_compaction(
            &dir,
            &compaction,
            &version,
            &mut id_gen,
            crate::internal_key::MAX_SEQUENCE,
            4 * 1024,
            10,
        )
        .unwrap();
        // 用迭代器数 entry：应该只有 1 条（最新 v3）。
        let reader = TableReader::open(&crate::file_meta::sst_path(
            &dir,
            output.new_files[0].number,
        ))
        .unwrap();
        let entries: Vec<_> = reader.into_table_iter().unwrap().collect();
        assert_eq!(entries.len(), 1, "old versions should be dropped");
        assert_eq!(entries[0].1, b"v3");
    }

    #[test]
    fn compaction_drops_tombstone_at_base_level() {
        // 删除标记 + is_base_level=true → 不写入输出。
        // 构造：L0 有 k 的删除标记，无更高层 → is_base=true。
        let dir = tmp_compaction_dir("droptomb");
        // MemTable.delete 写入删除标记。
        let mut mem = MemTable::new();
        mem.put(b"k", b"v");
        mem.delete(b"k");
        let path = crate::file_meta::sst_path(&dir, FileNumber(1));
        let result = mem.flush_to_sstable_with_bounds(&path).unwrap();
        let f1 = FileMetaData::new(
            FileNumber(1),
            std::fs::metadata(&path).unwrap().len(),
            result.smallest.unwrap(),
            result.largest.unwrap(),
        );
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1], vec![]],
            grandparents: vec![],
        };
        let mut id_gen = IdGenerator::new(100);
        let version = Version::empty(); // 无更高层 → is_base=true
        let output = do_compaction(
            &dir,
            &compaction,
            &version,
            &mut id_gen,
            crate::internal_key::MAX_SEQUENCE,
            4 * 1024,
            10,
        )
        .unwrap();
        // 删除标记被丢弃 + 唯一 entry 是删除标记 → 无 entry 写入 → 输出文件为空或不存在。
        // LevelDB 行为：空输出不产生文件。我们的实现可能产生空文件（finish 空 builder）。
        // 两种都接受：关键是读回时 get(k) 返回 None。
        if let Some(f) = output.new_files.first() {
            let reader = TableReader::open(&crate::file_meta::sst_path(&dir, f.number)).unwrap();
            assert_eq!(
                reader.get(b"k").unwrap(),
                None,
                "tombstone should be dropped at base level"
            );
        }
        // 没有新文件也是正确的（全被丢弃）。
    }

    /// 验证 do_compaction 使用传入的 block_target 参数（非硬编码默认值）。
    /// 小 block_target → 输出 SSTable 的 data block 数更多。
    #[test]
    fn do_compaction_respects_block_target() {
        let dir = tmp_compaction_dir("block_target");
        // 1000 条 entry，足够在 256 字节 block_target 下产生多个 data block。
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..1000u32)
            .map(|i| (format!("key{i:05}").into_bytes(), format!("v{i}").into_bytes()))
            .collect();
        let f1 = build_sst(&dir, FileNumber(1), 0, &entries);
        let compaction = Compaction {
            level: 0,
            inputs: [vec![f1], vec![]],
            grandparents: vec![],
        };
        let version = Version::empty();

        // 极小 block_target (128 字节) → 大量 data block
        let mut id_gen = IdGenerator::new(100);
        let out_small = do_compaction(
            &dir, &compaction, &version, &mut id_gen,
            crate::internal_key::MAX_SEQUENCE, 128, 10,
        ).unwrap();
        let reader_small = TableReader::open(&sst_path(&dir, out_small.new_files[0].number)).unwrap();
        let small_blocks = reader_small.into_table_iter().unwrap().blocks_loaded_capacity();

        // 大 block_target (64KB) → 少量 data block
        let mut id_gen2 = IdGenerator::new(200);
        let out_large = do_compaction(
            &dir, &compaction, &version, &mut id_gen2,
            crate::internal_key::MAX_SEQUENCE, 64 * 1024, 10,
        ).unwrap();
        let reader_large = TableReader::open(&sst_path(&dir, out_large.new_files[0].number)).unwrap();
        let large_blocks = reader_large.into_table_iter().unwrap().blocks_loaded_capacity();

        assert!(
            small_blocks > large_blocks,
            "small block_target (128) produced {small_blocks} data blocks, \
             large block_target (64KB) produced {large_blocks}; \
             small should have more blocks"
        );
    }
}
