//! `VersionEdit`：数据库状态的一次变更事件，手写 tag-based 二进制序列化。
//!
//! 为什么用"事件 edit"而非"全量快照"：manifest 体积可控、增量可回放；
//! 每次 flush/compaction append 一条 edit，崩溃恢复时从头回放累积成当前 Version。
//!
//! 编码格式：每个字段前缀 1 字节 tag（值忠于 LevelDB），后跟 varint / length-prefixed 数据。
//! 缺省字段（`None` 或空 `Vec`）不写出，解码时保持缺省。未知 tag 视为损坏（不做向前兼容预留）。

use crate::error::{MulanError, Result};
use crate::file_meta::{
    current_path, current_tmp_path, manifest_path, FileMetaData, FileNumber, MANIFEST_PREFIX,
};
use crate::internal_key::InternalKey;
use crate::varint::{decode_varint32, decode_varint64, encode_varint32, encode_varint64};
use crate::wal::{WalReader, WalWriter};
use std::io::Write;
use std::path::Path;

// tag 值忠于 LevelDB version_edit.cc。
const TAG_COMPARATOR: u8 = 1;
const TAG_LOG_NUMBER: u8 = 2;
const TAG_NEXT_FILE_NUMBER: u8 = 3;
const TAG_LAST_SEQUENCE: u8 = 4;
// TAG_COMPACT_POINTER = 5：compaction 轮转起点，未实现 compaction 时不出现在 edit 中。
const TAG_DELETED_FILE: u8 = 6;
const TAG_NEW_FILE: u8 = 7;
// 8 曾用于 large value ref，已废弃。
const TAG_PREV_LOG_NUMBER: u8 = 9;

/// 数据库状态的一次变更。
///
/// 标量字段用 `Option`：只有被设过值的字段才写进 manifest，解码时未出现的 tag 对应字段保持 `None`。
/// `deleted_files` / `new_files` 是变更集合，可为空。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VersionEdit {
    pub comparator: Option<String>,
    pub log_number: Option<u64>,
    pub prev_log_number: Option<u64>,
    pub next_file_number: Option<u64>,
    pub last_sequence: Option<u64>,
    pub deleted_files: Vec<DeletedFile>,
    pub new_files: Vec<NewFile>,
}

/// "从某层删除某文件"的记录。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeletedFile {
    pub level: u32,
    pub number: FileNumber,
}

/// "向某层新增某文件"的记录（携带完整 FileMetaData）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewFile {
    pub level: u32,
    pub meta: FileMetaData,
}

impl VersionEdit {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_comparator(&mut self, c: impl Into<String>) -> &mut Self {
        self.comparator = Some(c.into());
        self
    }

    pub fn set_log_number(&mut self, n: u64) -> &mut Self {
        self.log_number = Some(n);
        self
    }

    pub fn set_prev_log_number(&mut self, n: u64) -> &mut Self {
        self.prev_log_number = Some(n);
        self
    }

    pub fn set_next_file_number(&mut self, n: u64) -> &mut Self {
        self.next_file_number = Some(n);
        self
    }

    pub fn set_last_sequence(&mut self, s: u64) -> &mut Self {
        self.last_sequence = Some(s);
        self
    }

    pub fn add_delete(&mut self, level: u32, number: FileNumber) -> &mut Self {
        self.deleted_files.push(DeletedFile { level, number });
        self
    }

    pub fn add_file(&mut self, level: u32, meta: FileMetaData) -> &mut Self {
        self.new_files.push(NewFile { level, meta });
        self
    }

    /// 编码为字节。缺省字段不写出。
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.encode_to(&mut buf);
        buf
    }

    pub fn encode_to(&self, buf: &mut Vec<u8>) {
        if let Some(c) = &self.comparator {
            buf.push(TAG_COMPARATOR);
            encode_varint32(buf, c.len() as u32);
            buf.extend_from_slice(c.as_bytes());
        }
        if let Some(n) = self.log_number {
            buf.push(TAG_LOG_NUMBER);
            encode_varint64(buf, n);
        }
        if let Some(n) = self.prev_log_number {
            buf.push(TAG_PREV_LOG_NUMBER);
            encode_varint64(buf, n);
        }
        if let Some(n) = self.next_file_number {
            buf.push(TAG_NEXT_FILE_NUMBER);
            encode_varint64(buf, n);
        }
        if let Some(s) = self.last_sequence {
            buf.push(TAG_LAST_SEQUENCE);
            encode_varint64(buf, s);
        }
        for d in &self.deleted_files {
            buf.push(TAG_DELETED_FILE);
            encode_varint32(buf, d.level);
            encode_varint64(buf, d.number.0);
        }
        for f in &self.new_files {
            buf.push(TAG_NEW_FILE);
            encode_varint32(buf, f.level);
            encode_varint64(buf, f.meta.number.0);
            encode_varint64(buf, f.meta.file_size);
            // smallest / largest 用 InternalKey::encode() 后 length-prefix。
            let s = f.meta.smallest.encode();
            encode_varint32(buf, s.len() as u32);
            buf.extend_from_slice(&s);
            let l = f.meta.largest.encode();
            encode_varint32(buf, l.len() as u32);
            buf.extend_from_slice(&l);
        }
    }

    /// 从字节解码。遇到未知 tag 返回 `Corrupted`。
    pub fn decode(buf: &[u8]) -> Result<Self> {
        let mut edit = VersionEdit::new();
        let mut i = 0;
        while i < buf.len() {
            let tag = buf[i];
            i += 1;
            match tag {
                TAG_COMPARATOR => {
                    let (len, n) = decode_varint32(&buf[i..])?;
                    i += n;
                    let end = i + len as usize;
                    if end > buf.len() {
                        return Err(corrupt("comparator length out of bounds"));
                    }
                    edit.comparator = Some(
                        String::from_utf8(buf[i..end].to_vec())
                            .map_err(|e| corrupt(format!("comparator not utf-8: {e}")))?,
                    );
                    i = end;
                }
                TAG_LOG_NUMBER => {
                    let (n, used) = decode_varint64(&buf[i..])?;
                    i += used;
                    edit.log_number = Some(n);
                }
                TAG_PREV_LOG_NUMBER => {
                    let (n, used) = decode_varint64(&buf[i..])?;
                    i += used;
                    edit.prev_log_number = Some(n);
                }
                TAG_NEXT_FILE_NUMBER => {
                    let (n, used) = decode_varint64(&buf[i..])?;
                    i += used;
                    edit.next_file_number = Some(n);
                }
                TAG_LAST_SEQUENCE => {
                    let (s, used) = decode_varint64(&buf[i..])?;
                    i += used;
                    edit.last_sequence = Some(s);
                }
                TAG_DELETED_FILE => {
                    let (level, n1) = decode_varint32(&buf[i..])?;
                    i += n1;
                    let (num, n2) = decode_varint64(&buf[i..])?;
                    i += n2;
                    edit.deleted_files.push(DeletedFile {
                        level,
                        number: FileNumber(num),
                    });
                }
                TAG_NEW_FILE => {
                    let (level, n1) = decode_varint32(&buf[i..])?;
                    i += n1;
                    let (num, n2) = decode_varint64(&buf[i..])?;
                    i += n2;
                    let (size, n3) = decode_varint64(&buf[i..])?;
                    i += n3;
                    let (slen, n4) = decode_varint32(&buf[i..])?;
                    i += n4;
                    let s_end = i + slen as usize;
                    if s_end > buf.len() {
                        return Err(corrupt("smallest key length out of bounds"));
                    }
                    let smallest = InternalKey::decode(&buf[i..s_end])?;
                    i = s_end;
                    let (llen, n5) = decode_varint32(&buf[i..])?;
                    i += n5;
                    let l_end = i + llen as usize;
                    if l_end > buf.len() {
                        return Err(corrupt("largest key length out of bounds"));
                    }
                    let largest = InternalKey::decode(&buf[i..l_end])?;
                    i = l_end;
                    edit.new_files.push(NewFile {
                        level,
                        // allowed_seeks 不持久化，解码时初始化为 0。
                        meta: FileMetaData::new(FileNumber(num), size, smallest, largest),
                    });
                }
                _ => return Err(corrupt(format!("unknown version edit tag: {tag}"))),
            }
        }
        Ok(edit)
    }
}

fn corrupt(msg: impl Into<String>) -> MulanError {
    MulanError::Corrupted(format!("version edit: {}", msg.into()))
}

/// Manifest 写入器。内部复用 WAL 的 log 格式（crc32c + 分片），
/// 保证每条 VersionEdit 落盘后可校验、崩溃可定位到完整 record。
pub struct ManifestWriter {
    wal: WalWriter,
}

impl ManifestWriter {
    /// 打开（或创建）指定编号的 manifest 文件，以追加模式写入。
    pub fn create(dir: &Path, number: FileNumber) -> Result<Self> {
        let path = manifest_path(dir, number);
        let wal = WalWriter::create(&path)?;
        Ok(ManifestWriter { wal })
    }

    /// 追加一条 VersionEdit 并立即落盘。
    ///
    /// manifest 是 flush/compaction 的提交点：sync 成功后变更才视为生效；
    /// sync 前崩溃则该条 edit 丢失，恢复时回退到上一条已落盘的状态。
    pub fn append(&mut self, edit: &VersionEdit) -> Result<()> {
        let bytes = edit.encode();
        self.wal.add_record(&bytes)?;
        self.wal.sync()
    }
}

/// 原子写入 CURRENT 文件，指向当前 manifest。
///
/// 流程：写 `CURRENT.dbtmp`（内容 = manifest 文件名 + `\n`）→ fsync → rename 到 `CURRENT`。
/// 同目录 rename 对崩溃是原子的（POSIX 与 Windows 的 `std::fs::rename` 均可替换已存在目标），
/// 故崩溃时 CURRENT 要么是旧值要么是新值，不会出现半截内容。
pub fn write_current(dir: &Path, manifest_number: FileNumber) -> Result<()> {
    let tmp = current_tmp_path(dir);
    let name = format!("{MANIFEST_PREFIX}{}", manifest_number.to_name_part());
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(name.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, current_path(dir))?;
    Ok(())
}

/// 读取 CURRENT，返回它指向的 manifest 编号。
pub fn read_current(dir: &Path) -> Result<FileNumber> {
    let content = std::fs::read_to_string(current_path(dir))?;
    // 去掉尾部换行/空白。manifest 名本身不含空白，trim_end 安全。
    let name = content.trim_end();
    crate::file_meta::parse_manifest_name(name)
        .ok_or_else(|| corrupt(format!("CURRENT points to invalid manifest name: {name:?}")))
}

/// Manifest 恢复结果：累积的 VersionEdit 列表 + 当前 manifest 编号。
pub struct ManifestRecovery {
    pub edits: Vec<VersionEdit>,
    pub manifest_number: FileNumber,
}

/// 从 CURRENT 指向的 manifest 回放所有 VersionEdit。
///
/// 复用 `WalReader`：遇到坏 record 自动停止，故崩溃时末尾残片被丢弃，
/// 恢复到最近一条完整落盘的 edit。
pub fn recover_manifest(dir: &Path) -> Result<ManifestRecovery> {
    let manifest_number = read_current(dir)?;
    let path = manifest_path(dir, manifest_number);
    let reader = WalReader::open(&path)?;
    let records = reader.read_records()?;
    let mut edits = Vec::with_capacity(records.len());
    for rec in records {
        edits.push(VersionEdit::decode(&rec)?);
    }
    Ok(ManifestRecovery {
        edits,
        manifest_number,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_key::ValueType;

    fn meta(num: u64, size: u64, a: &[u8], b: &[u8]) -> FileMetaData {
        FileMetaData::new(
            FileNumber(num),
            size,
            InternalKey::new(a.to_vec(), 1, ValueType::Put),
            InternalKey::new(b.to_vec(), 2, ValueType::Put),
        )
    }

    #[test]
    fn empty_edit_round_trips() {
        let edit = VersionEdit::new();
        let bytes = edit.encode();
        assert!(bytes.is_empty(), "empty edit encodes to zero bytes");
        assert_eq!(VersionEdit::decode(&bytes).unwrap(), edit);
    }

    #[test]
    fn scalar_fields_round_trip() {
        let mut edit = VersionEdit::new();
        edit.set_comparator("leveldb.BytewiseComparator")
            .set_log_number(7)
            .set_prev_log_number(0)
            .set_next_file_number(9)
            .set_last_sequence(42);
        let bytes = edit.encode();
        assert_eq!(VersionEdit::decode(&bytes).unwrap(), edit);
    }

    #[test]
    fn omitted_fields_decode_as_none() {
        // 只设 last_sequence，其余保持 None。
        let mut edit = VersionEdit::new();
        edit.set_last_sequence(99);
        let decoded = VersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(decoded.last_sequence, Some(99));
        assert_eq!(decoded.log_number, None);
        assert_eq!(decoded.next_file_number, None);
        assert_eq!(decoded.comparator, None);
        assert!(decoded.new_files.is_empty());
    }

    #[test]
    fn new_and_deleted_files_round_trip() {
        let mut edit = VersionEdit::new();
        edit.add_file(0, meta(1, 4096, b"a", b"z"))
            .add_file(0, meta(2, 8192, b"m", b"y"))
            .add_delete(1, FileNumber(5))
            .add_delete(2, FileNumber(6))
            .set_last_sequence(100);
        let decoded = VersionEdit::decode(&edit.encode()).unwrap();
        assert_eq!(decoded, edit);
        // 顺序保持不变。
        assert_eq!(decoded.new_files.len(), 2);
        assert_eq!(decoded.new_files[0].meta.number, FileNumber(1));
        assert_eq!(decoded.new_files[1].meta.number, FileNumber(2));
        assert_eq!(
            decoded.deleted_files[0],
            DeletedFile {
                level: 1,
                number: FileNumber(5)
            }
        );
    }

    #[test]
    fn new_file_preserves_smallest_largest_internal_keys() {
        // smallest/largest 是带 seq/type 的 InternalKey，必须完整保留。
        let mut edit = VersionEdit::new();
        let m = FileMetaData::new(
            FileNumber(3),
            1024,
            InternalKey::new(b"key-a".to_vec(), 123, ValueType::Delete),
            InternalKey::new(b"key-z".to_vec(), 999, ValueType::Put),
        );
        edit.add_file(2, m);
        let decoded = VersionEdit::decode(&edit.encode()).unwrap();
        let got = &decoded.new_files[0].meta;
        assert_eq!(got.smallest.seq, 123);
        assert_eq!(got.smallest.vtype, ValueType::Delete);
        assert_eq!(got.largest.seq, 999);
        assert_eq!(got.smallest.user_key, b"key-a");
        assert_eq!(got.largest.user_key, b"key-z");
        // allowed_seeks 不持久化，解码重置为 0。
        assert_eq!(got.allowed_seeks, 0);
    }

    #[test]
    fn encoding_is_deterministic() {
        // 同一 edit 多次编码字节完全一致，便于断言 manifest 内容。
        let mut edit = VersionEdit::new();
        edit.add_file(0, meta(1, 100, b"a", b"b"))
            .set_last_sequence(5);
        let b1 = edit.encode();
        let b2 = edit.encode();
        assert_eq!(b1, b2);
    }

    #[test]
    fn decode_rejects_unknown_tag() {
        // tag=200 未知 → Corrupted。
        let bytes = [200u8];
        assert!(VersionEdit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncated_comparator_length() {
        // tag=1 但后面没有长度字节。
        let bytes = [TAG_COMPARATOR];
        assert!(VersionEdit::decode(&bytes).is_err());
    }

    #[test]
    fn decode_rejects_truncated_new_file() {
        // tag=7 后跟 level 但缺少后续字段。
        let mut buf = Vec::new();
        buf.push(TAG_NEW_FILE);
        encode_varint32(&mut buf, 0); // level
                                      // 缺 number/size/keys → decode_varint64 在空切片上失败。
        assert!(VersionEdit::decode(&buf).is_err());
    }

    #[test]
    fn decode_rejects_bad_comparator_utf8() {
        let mut buf = Vec::new();
        buf.push(TAG_COMPARATOR);
        encode_varint32(&mut buf, 2);
        buf.extend_from_slice(&[0xFF, 0xFE]); // 非 utf-8。
        assert!(VersionEdit::decode(&buf).is_err());
    }

    use std::path::PathBuf;

    fn tmp_dir(label: &str) -> PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "mulan-manifest-test-{}-{}-{}",
            std::process::id(),
            label,
            n
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn write_and_read_current_round_trip() {
        let dir = tmp_dir("current");
        write_current(&dir, FileNumber(7)).unwrap();
        assert_eq!(read_current(&dir).unwrap(), FileNumber(7));
        // 写第二次覆盖第一次。
        write_current(&dir, FileNumber(8)).unwrap();
        assert_eq!(read_current(&dir).unwrap(), FileNumber(8));
    }

    #[test]
    fn write_current_leaves_no_tmp_residue() {
        let dir = tmp_dir("tmp");
        write_current(&dir, FileNumber(3)).unwrap();
        assert!(
            !current_tmp_path(&dir).exists(),
            "CURRENT.dbtmp should be renamed away"
        );
        assert!(current_path(&dir).exists());
    }

    #[test]
    fn read_current_rejects_invalid_content() {
        let dir = tmp_dir("badcurrent");
        // 直接写一个非法内容（非 MANIFEST-NNNNNN 格式）到 CURRENT，模拟半截写。
        std::fs::write(current_path(&dir), "garbage-no-newline").unwrap();
        assert!(read_current(&dir).is_err());
        // 空内容也非法。
        std::fs::write(current_path(&dir), "").unwrap();
        assert!(read_current(&dir).is_err());
    }

    #[test]
    fn manifest_writer_append_and_recover() {
        let dir = tmp_dir("append");
        let mut writer = ManifestWriter::create(&dir, FileNumber(1)).unwrap();
        let mut e1 = VersionEdit::new();
        e1.set_comparator("bytewise")
            .set_log_number(10)
            .set_last_sequence(5);
        let mut e2 = VersionEdit::new();
        e2.add_file(0, meta(11, 4096, b"a", b"z"))
            .set_last_sequence(20);
        let mut e3 = VersionEdit::new();
        e3.add_delete(0, FileNumber(11)).set_next_file_number(12);
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();
        writer.append(&e3).unwrap();
        write_current(&dir, FileNumber(1)).unwrap();

        let recovered = recover_manifest(&dir).unwrap();
        assert_eq!(recovered.manifest_number, FileNumber(1));
        assert_eq!(recovered.edits.len(), 3);
        assert_eq!(recovered.edits[0], e1);
        assert_eq!(recovered.edits[1], e2);
        assert_eq!(recovered.edits[2], e3);
    }

    #[test]
    fn recover_on_empty_current_errors() {
        // 新库没有 CURRENT，recover_manifest 应失败（由上层走"新建库"路径）。
        let dir = tmp_dir("empty");
        assert!(recover_manifest(&dir).is_err());
    }

    #[test]
    fn recover_stops_at_truncated_record() {
        // 写两条 edit 后，人为截断 manifest 文件末尾，模拟崩溃残片。
        let dir = tmp_dir("trunc");
        let mut writer = ManifestWriter::create(&dir, FileNumber(1)).unwrap();
        let mut e1 = VersionEdit::new();
        e1.set_last_sequence(1);
        let mut e2 = VersionEdit::new();
        e2.set_last_sequence(2);
        writer.append(&e1).unwrap();
        writer.append(&e2).unwrap();
        write_current(&dir, FileNumber(1)).unwrap();
        drop(writer);

        let path = manifest_path(&dir, FileNumber(1));
        let len = std::fs::metadata(&path).unwrap().len();
        // 截掉末尾若干字节，破坏最后一条 record。
        let truncated = len / 2;
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(truncated)
            .unwrap();

        // 恢复应成功但只拿到完整 record（至少第一条 e1）。
        let recovered = recover_manifest(&dir).unwrap();
        assert!(recovered.edits.len() <= 2);
        // 第一条完整落盘的应能恢复。
        assert!(!recovered.edits.is_empty() || truncated == 0);
    }
}
