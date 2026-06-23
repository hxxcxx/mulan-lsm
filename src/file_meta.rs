//! 文件命名规范 + `FileMetaData` + 编号分配器。
//!
//! 所有持久化文件用全局单调递增的 6 位数字编号，编号即创建序：
//!   - `<NNNNNN>.log`        WAL 日志
//!   - `<NNNNNN>.sst`        SSTable
//!   - `MANIFEST-<NNNNNN>`   Manifest（不用点号分隔，前缀区分）
//!   - `CURRENT`             指向当前 manifest 的纯文本文件
//!
//! 单调编号的好处：文件名天然反映创建顺序，恢复时易定位"最新的"；
//! 编号永不复用，避免新旧文件混淆。LevelDB 即此方案。

use crate::internal_key::InternalKey;
use std::path::{Path, PathBuf};

/// 全局文件编号。newtype 包一层，避免与普通 u64（如 sequence）混用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileNumber(pub u64);

impl FileNumber {
    /// 渲染成 6 位零填充字符串，用于文件名。编号超过 6 位时自然扩展（不截断）。
    pub fn to_name_part(self) -> String {
        format!("{:06}", self.0)
    }
}

impl std::fmt::Display for FileNumber {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:06}", self.0)
    }
}

/// SSTable 文件后缀。
pub const SST_SUFFIX: &str = "sst";
/// WAL 文件后缀。
pub const LOG_SUFFIX: &str = "log";
/// 临时文件后缀（原子写 CURRENT 时的中转文件）。
pub const TMP_SUFFIX: &str = "dbtmp";
/// Manifest 文件名前缀。
pub const MANIFEST_PREFIX: &str = "MANIFEST-";
/// CURRENT 文件名。
pub const CURRENT_NAME: &str = "CURRENT";

/// 拼一个普通编号文件的完整路径：`<dir>/<NNNNNN>.<suffix>`。
pub fn numbered_file_path(dir: &Path, number: FileNumber, suffix: &str) -> PathBuf {
    dir.join(format!("{}.{}", number.to_name_part(), suffix))
}

/// SSTable 路径：`<dir>/<NNNNNN>.sst`。
pub fn sst_path(dir: &Path, number: FileNumber) -> PathBuf {
    numbered_file_path(dir, number, SST_SUFFIX)
}

/// WAL 路径：`<dir>/<NNNNNN>.log`。
pub fn log_path(dir: &Path, number: FileNumber) -> PathBuf {
    numbered_file_path(dir, number, LOG_SUFFIX)
}

/// Manifest 路径：`<dir>/MANIFEST-<NNNNNN>`。
pub fn manifest_path(dir: &Path, number: FileNumber) -> PathBuf {
    dir.join(format!("{MANIFEST_PREFIX}{}", number.to_name_part()))
}

/// CURRENT 文件路径：`<dir>/CURRENT`。
pub fn current_path(dir: &Path) -> PathBuf {
    dir.join(CURRENT_NAME)
}

/// CURRENT 写入的中转临时文件路径：`<dir>/CURRENT.dbtmp`。
pub fn current_tmp_path(dir: &Path) -> PathBuf {
    dir.join(format!("{CURRENT_NAME}.{TMP_SUFFIX}"))
}

/// 从 SSTable 文件名解析编号。形如 `000123.sst` → `FileNumber(123)`。
/// 非法格式返回 None。
pub fn parse_sst_name(name: &str) -> Option<FileNumber> {
    let stem = name.strip_suffix(".sst")?;
    parse_number(stem)
}

/// 从 WAL 文件名解析编号。形如 `000123.log` → `FileNumber(123)`。
pub fn parse_log_name(name: &str) -> Option<FileNumber> {
    let stem = name.strip_suffix(".log")?;
    parse_number(stem)
}

/// 从 Manifest 文件名解析编号。形如 `MANIFEST-000123` → `FileNumber(123)`。
pub fn parse_manifest_name(name: &str) -> Option<FileNumber> {
    let stem = name.strip_prefix(MANIFEST_PREFIX)?;
    parse_number(stem)
}

/// 解析纯数字字符串为 FileNumber。允许前导零，要求全数字且非空。
fn parse_number(s: &str) -> Option<FileNumber> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    s.parse::<u64>().ok().map(FileNumber)
}

/// 单个 SSTable 的元数据。`Version` / `VersionEdit` / `Compaction` 三处共用同一组字段。
///
/// `smallest`/`largest` 是该文件内 internal key 的最小/最大值（按 `InternalKey` Ord），
/// 用于 L1+ 的二分定位和 compaction 选文件。`allowed_seeks` 是 compaction 的 seek 节流计数，
/// 记录该文件被 point-seek 的次数，超过阈值时触发该文件所在区间的 compaction。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileMetaData {
    pub number: FileNumber,
    pub file_size: u64,
    pub smallest: InternalKey,
    pub largest: InternalKey,
    pub allowed_seeks: u64,
}

impl FileMetaData {
    pub fn new(
        number: FileNumber,
        file_size: u64,
        smallest: InternalKey,
        largest: InternalKey,
    ) -> Self {
        FileMetaData {
            number,
            file_size,
            smallest,
            largest,
            allowed_seeks: 0,
        }
    }
}

/// 全局编号分配器。单调递增，永不回退。
///
/// 单线程使用；多线程场景需换成原子实现以支持后台线程并发分配。
#[derive(Debug)]
pub struct IdGenerator {
    next: u64,
}

impl IdGenerator {
    /// 从给定起点开始分配。新库从 1 起（0 保留给"无 WAL"哨兵，见 4.4）。
    pub fn new(start: u64) -> Self {
        IdGenerator { next: start }
    }

    /// 分配下一个编号。
    pub fn new_file_number(&mut self) -> FileNumber {
        let n = self.next;
        self.next += 1;
        FileNumber(n)
    }

    /// 下一个将分配的编号（= 已分配最大编号 + 1）。
    pub fn next_number(&self) -> u64 {
        self.next
    }

    /// 恢复时把分配器推进到至少 `n`，保证不重用已存在的文件编号。
    pub fn bump_to(&mut self, n: u64) {
        if n > self.next {
            self.next = n;
        }
    }
}

impl Default for IdGenerator {
    fn default() -> Self {
        IdGenerator::new(1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_key::ValueType;

    fn ik(s: &[u8], seq: u64) -> InternalKey {
        InternalKey::new(s.to_vec(), seq, ValueType::Put)
    }

    #[test]
    fn file_number_pads_to_six_digits() {
        assert_eq!(FileNumber(1).to_name_part(), "000001");
        assert_eq!(FileNumber(123).to_name_part(), "000123");
        assert_eq!(FileNumber(0).to_name_part(), "000000");
    }

    #[test]
    fn file_number_extends_beyond_six_digits() {
        // 编号超过 6 位时自然扩展，不截断。
        assert_eq!(FileNumber(1_000_000).to_name_part(), "1000000");
    }

    #[test]
    fn path_builders_match_naming_convention() {
        let dir = Path::new("/db");
        assert_eq!(
            sst_path(dir, FileNumber(7)),
            PathBuf::from("/db/000007.sst")
        );
        assert_eq!(
            log_path(dir, FileNumber(7)),
            PathBuf::from("/db/000007.log")
        );
        assert_eq!(
            manifest_path(dir, FileNumber(5)),
            PathBuf::from("/db/MANIFEST-000005")
        );
        assert_eq!(current_path(dir), PathBuf::from("/db/CURRENT"));
        assert_eq!(current_tmp_path(dir), PathBuf::from("/db/CURRENT.dbtmp"));
    }

    #[test]
    fn parse_sst_name_round_trip() {
        assert_eq!(parse_sst_name("000123.sst"), Some(FileNumber(123)));
        assert_eq!(parse_sst_name("000000.sst"), Some(FileNumber(0)));
        assert_eq!(parse_sst_name("1000000.sst"), Some(FileNumber(1_000_000)));
        // 非法：缺后缀、非数字、空。
        assert_eq!(parse_sst_name("000123.log"), None);
        assert_eq!(parse_sst_name("abc.sst"), None);
        assert_eq!(parse_sst_name(".sst"), None);
        assert_eq!(parse_sst_name("000123.sst.bak"), None);
    }

    #[test]
    fn parse_log_name_round_trip() {
        assert_eq!(parse_log_name("000042.log"), Some(FileNumber(42)));
        assert_eq!(parse_log_name("000042.sst"), None);
    }

    #[test]
    fn parse_manifest_name_round_trip() {
        assert_eq!(parse_manifest_name("MANIFEST-000005"), Some(FileNumber(5)));
        assert_eq!(parse_manifest_name("MANIFEST-000042"), Some(FileNumber(42)));
        // 缺前缀或后缀非法。
        assert_eq!(parse_manifest_name("000005"), None);
        assert_eq!(parse_manifest_name("MANIFEST-"), None);
        assert_eq!(parse_manifest_name("MANIFEST-abc"), None);
    }

    #[test]
    fn id_generator_is_strictly_monotonic() {
        let mut gen = IdGenerator::new(1);
        assert_eq!(gen.new_file_number(), FileNumber(1));
        assert_eq!(gen.new_file_number(), FileNumber(2));
        assert_eq!(gen.new_file_number(), FileNumber(3));
        assert_eq!(gen.next_number(), 4);
    }

    #[test]
    fn id_generator_bump_to_only_grows() {
        let mut gen = IdGenerator::new(5);
        gen.bump_to(10);
        assert_eq!(gen.next_number(), 10);
        // bump 到更小值无效，不回退。
        gen.bump_to(3);
        assert_eq!(gen.next_number(), 10);
    }

    #[test]
    fn file_meta_data_construction() {
        let meta = FileMetaData::new(FileNumber(9), 4096, ik(b"a", 1), ik(b"z", 2));
        assert_eq!(meta.number, FileNumber(9));
        assert_eq!(meta.file_size, 4096);
        assert_eq!(meta.smallest, ik(b"a", 1));
        assert_eq!(meta.largest, ik(b"z", 2));
        assert_eq!(meta.allowed_seeks, 0);
    }

    #[test]
    fn file_number_orders_numerically() {
        // 排序应反映数值大小，而非字符串字典序（"000010" > "000009" 字典序也对，但确认一下）。
        assert!(FileNumber(9) < FileNumber(10));
        assert!(FileNumber(255) < FileNumber(256));
    }
}
