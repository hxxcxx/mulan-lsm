//! SSTable 的整体组装（Builder）与读取（Reader）。
//!
//! 文件布局：
//! ```text
//! [data block 0][data block 1]...[data block N-1]
//! [metaindex block]   ← 第一版为空 block（无 filter），占位
//! [index block]       ← 每 data block 一条 (最大 key 的 sort_key, handle)
//! [footer]            ← 固定长度，含 metaindex/index handle + magic
//! ```
//!
//! data block / index block 都用 M3.1 的 Block 格式（纯字节 + 前缀压缩）。
//! key 存的是 InternalKey 的 sort_key（保证字节字典序 == Ord）。

use crate::error::{MulanError, Result};
use crate::sstable::block::{Block, BlockBuilder};
use crate::varint::{decode_varint64, encode_varint64};
use std::io::Write;

/// data block 目标大小。达到后切一个新 block。LevelDB 默认 4KB。
const DATA_BLOCK_TARGET: usize = 4 * 1024;

/// footer 固定长度。两个 handle（变长）+ padding + magic(8)。
/// LevelDB 是 48 字节；这里用同样固定长度，padding 补齐。
const FOOTER_LEN: usize = 48;
/// footer 尾部 magic，用于校验文件是 SSTable。
const MAGIC: u64 = 0xdb47_7524_8b80_fb57;

/// 指向文件中某 block 的位置：偏移 + 大小。都用 varint64 编码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint64(buf, self.offset);
        encode_varint64(buf, self.size);
    }

    pub fn decode(buf: &[u8]) -> Result<(Self, usize)> {
        let (offset, n1) = decode_varint64(buf)?;
        let (size, n2) = decode_varint64(&buf[n1..])?;
        Ok((BlockHandle { offset, size }, n1 + n2))
    }
}

/// SSTable 构造器。按 key 有序（sort_key 字节序）追加 kv，自动切 data block、维护 index。
pub struct TableBuilder {
    file: std::fs::File,
    /// 当前正在累积的 data block。
    data_block: BlockBuilder,
    /// index block：每条 (上一个 data block 的最大 sort_key, 该 block 的 handle)。
    index_block: BlockBuilder,
    /// 当前已写入文件的字节数（= 下个 block 的起始 offset）。
    file_offset: u64,
    /// 最近一次 add 的 sort_key（即当前 data block 的最大 key）。
    last_key: Vec<u8>,
    /// 上一个 data block 已落盘但 index 项还没写入：记下它的最大 key 和 handle。
    pending_index_key: Option<Vec<u8>>,
    pending_handle: Option<BlockHandle>,
    /// 已写入的 entry 数。
    num_entries: u64,
    finished: bool,
}

impl TableBuilder {
    pub fn new(file: std::fs::File) -> Self {
        TableBuilder {
            file,
            data_block: BlockBuilder::new(),
            index_block: BlockBuilder::new(),
            file_offset: 0,
            last_key: Vec::new(),
            pending_index_key: None,
            pending_handle: None,
            num_entries: 0,
            finished: false,
        }
    }

    /// 追加一条 (sort_key, value)。sort_key 必须严格大于上一条（字节升序）。
    pub fn add(&mut self, sort_key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished, "TableBuilder already finished");

        // 若上一个 data block 已切出但 index 项还没记录，现在用它的最大 key 补上。
        if let Some(key) = self.pending_index_key.take() {
            if let Some(handle) = self.pending_handle.take() {
                let mut handle_buf = Vec::new();
                handle.encode(&mut handle_buf);
                self.index_block.add(&key, &handle_buf);
            }
        }

        self.data_block.add(sort_key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(sort_key);
        self.num_entries += 1;

        // data block 攒够大小，切出并写盘。
        if self.data_block.current_size_estimate() >= DATA_BLOCK_TARGET {
            self.flush_data_block()?;
        }
        Ok(())
    }

    /// 把当前 data_block 落盘，记下 handle 和"本 block 最大 key"待写入 index。
    fn flush_data_block(&mut self) -> Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let bytes = std::mem::take(&mut self.data_block).finish();
        let handle = self.write_raw(&bytes)?;
        // 用本 block 的最大 key（last_key）作为 index 分隔符。
        self.pending_index_key = Some(self.last_key.clone());
        self.pending_handle = Some(handle);
        Ok(())
    }

    /// 写一段原始字节到文件，返回它的 handle。
    fn write_raw(&mut self, bytes: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.file_offset,
            size: bytes.len() as u64,
        };
        self.file.write_all(bytes)?;
        self.file_offset += bytes.len() as u64;
        Ok(handle)
    }

    /// 完成 SSTable：flush 最后一个 data block、写 index、写 footer。
    pub fn finish(mut self) -> Result<()> {
        assert!(!self.finished, "TableBuilder already finished");
        self.finished = true;

        // 刷出最后的 data block。
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }
        // 补最后一个 pending index 项。
        if let Some(key) = self.pending_index_key.take() {
            if let Some(handle) = self.pending_handle.take() {
                let mut handle_buf = Vec::new();
                handle.encode(&mut handle_buf);
                self.index_block.add(&key, &handle_buf);
            }
        }

        // 写 metaindex block（第一版空 block，占位）。
        let metaindex_bytes = BlockBuilder::new().finish();
        let metaindex_handle = self.write_raw(&metaindex_bytes)?;

        // 写 index block。
        let index_bytes = std::mem::take(&mut self.index_block).finish();
        let index_handle = self.write_raw(&index_bytes)?;

        // 写 footer。
        let mut footer = Vec::with_capacity(FOOTER_LEN);
        metaindex_handle.encode(&mut footer);
        index_handle.encode(&mut footer);
        footer.resize(FOOTER_LEN - 8, 0);
        footer.extend_from_slice(&MAGIC.to_le_bytes());
        assert_eq!(footer.len(), FOOTER_LEN);
        self.file.write_all(&footer)?;

        Ok(())
    }

    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
}

/// SSTable 读取器。打开文件后支持 get(sort_key)。
pub struct TableReader {
    /// 整个文件读进内存。第一版简化（大 SSTable 可改为按需读 block）。
    data: Vec<u8>,
    index_handle: BlockHandle,
}

impl TableReader {
    /// 打开文件并解析 footer。
    pub fn open(path: &std::path::Path) -> Result<Self> {
        let data = std::fs::read(path)?;
        if data.len() < FOOTER_LEN {
            return Err(MulanError::Corrupted(format!(
                "file too short for footer: {} bytes",
                data.len()
            )));
        }
        // footer 在文件末尾 FOOTER_LEN 字节。
        let footer = &data[data.len() - FOOTER_LEN..];
        // 校验 magic。
        let magic_bytes: [u8; 8] = footer[FOOTER_LEN - 8..].try_into().unwrap();
        let magic = u64::from_le_bytes(magic_bytes);
        if magic != MAGIC {
            return Err(MulanError::Corrupted(format!(
                "bad magic: 0x{magic:x}, expected 0x{MAGIC:x}"
            )));
        }
        // 解析两个 handle（metaindex 在前，index 在后）。
        let (_metaindex_handle, n1) = BlockHandle::decode(footer)?;
        let (index_handle, _n2) = BlockHandle::decode(&footer[n1..])?;
        Ok(TableReader { data, index_handle })
    }

    /// 查找 sort_key。返回命中 value 的字节切片借用。
    pub fn get(&self, sort_key: &[u8]) -> Option<&[u8]> {
        // 1. 读 index block，lower_bound 定位 data block。
        let index_bytes = self.block_bytes(&self.index_handle).ok()?;
        let index_block = Block::new(index_bytes).ok()?;
        // index block 的 key 是各 data block 的"最大 sort_key"分隔符。
        // lower_bound 找第一个分隔符 >= target → 对应覆盖该 key 的 data block。
        // 用 lower_bound 而非 get：target 可能小于首个分隔符，此时应命中第一个 data block。
        let data_handle_bytes = index_block.lower_bound(sort_key, &|a, b| a.cmp(b))?;

        // data_handle_bytes 是 varint 编码的 BlockHandle。
        let (data_handle, _n) = BlockHandle::decode(data_handle_bytes).ok()?;

        // 2. 读 data block，块内查找。
        let data_block_bytes = self.block_bytes(&data_handle).ok()?;
        let data_block = Block::new(data_block_bytes).ok()?;
        data_block.get(sort_key, &|a, b| a.cmp(b))
    }

    /// 取一个 block 的字节切片。
    fn block_bytes(&self, handle: &BlockHandle) -> Result<&[u8]> {
        let start = handle.offset as usize;
        let end = start + handle.size as usize;
        if end > self.data.len() {
            return Err(MulanError::Corrupted(format!(
                "block handle out of bounds: offset={} size={} file_len={}",
                handle.offset,
                handle.size,
                self.data.len()
            )));
        }
        Ok(&self.data[start..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_key::{InternalKey, ValueType, MAX_SEQUENCE};
    use std::path::PathBuf;

    static DIR_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn tmp_path(name: &str) -> PathBuf {
        let n = DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("mulan-sstable-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    /// 构造一组有序 InternalKey（按 Ord 升序），返回 (sort_key, value) 对。
    fn ordered_entries(count: u32, key_space: u32) -> Vec<(InternalKey, Vec<u8>)> {
        let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();
        // 每个 user_key 写 2 个版本（seq 递增），保证多版本在 sort_key 下正确排列。
        for i in 0..count {
            let user_key = (i % key_space).to_be_bytes().to_vec();
            let seq = (i / key_space + 1) as u64; // 同 user_key 的第二版本 seq 更大
            let ik = InternalKey::new(user_key, seq, ValueType::Put);
            entries.push((ik, format!("v{i}").into_bytes()));
        }
        // 按 Ord 升序排列。
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
    }

    #[test]
    fn round_trip_small_table() {
        let path = tmp_path("small.sst");
        let entries = ordered_entries(20, 10);

        // 写。
        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.sort_key(), value).unwrap();
        }
        assert_eq!(builder.num_entries(), 20);
        builder.finish().unwrap();

        // 读回，全部命中。
        let reader = TableReader::open(&path).unwrap();
        for (ik, value) in &entries {
            let got = reader.get(&ik.sort_key());
            assert_eq!(got, Some(value.as_slice()), "missed key {:?}", ik);
        }
    }

    #[test]
    fn round_trip_multi_data_block() {
        // 写入大量 key，触发多个 data block + index 多项。
        let path = tmp_path("multi.sst");
        let entries = ordered_entries(5_000, 1_000);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.sort_key(), value).unwrap();
        }
        builder.finish().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > DATA_BLOCK_TARGET as u64);

        let reader = TableReader::open(&path).unwrap();
        // 全量命中。
        for (ik, value) in &entries {
            assert_eq!(reader.get(&ik.sort_key()), Some(value.as_slice()));
        }
        // 未命中：构造一个 user_key 不存在、但 sort_key 落在区间内的 key。
        let missing = InternalKey::new(u32::MAX.to_be_bytes().to_vec(), 1, ValueType::Put);
        assert_eq!(reader.get(&missing.sort_key()), None);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let path = tmp_path("missing.sst");
        let entries = ordered_entries(5, 5);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.sort_key(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        // 完全不存在的 user_key。
        let ghost = InternalKey::new(b"zzz-not-exist".to_vec(), 1, ValueType::Put);
        assert_eq!(reader.get(&ghost.sort_key()), None);
        // 存在的 user_key 但 seq 极小（sort_key 排在最前，比所有真实版本都小）。
        let low_seq = InternalKey::new(
            0u32.to_be_bytes().to_vec(),
            MAX_SEQUENCE + 1,
            ValueType::Put,
        );
        let _ = low_seq; // MAX_SEQUENCE+1 不合法，仅占位说明。
    }

    #[test]
    fn empty_table_rejected() {
        // 空文件不是合法 SSTable（无 footer）。
        let path = tmp_path("empty.sst");
        std::fs::write(&path, b"").unwrap();
        assert!(TableReader::open(&path).is_err());
    }

    #[test]
    fn bad_magic_rejected() {
        let path = tmp_path("badmagic.sst");
        // 写一个 footer 长度但 magic 错的文件。
        let mut buf = vec![0u8; FOOTER_LEN];
        buf.extend_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
        std::fs::write(&path, &buf).unwrap();
        assert!(TableReader::open(&path).is_err());
    }

    #[test]
    fn delete_entries_stored_and_found() {
        // Delete 类型也能存进 SSTable（value 空），get 命中返回空切片。
        let path = tmp_path("delete.sst");
        let mut entries: Vec<(InternalKey, Vec<u8>)> = vec![
            (
                InternalKey::new(b"k1".to_vec(), 1, ValueType::Put),
                b"v1".to_vec(),
            ),
            (
                InternalKey::new(b"k1".to_vec(), 2, ValueType::Delete),
                Vec::new(),
            ),
            (
                InternalKey::new(b"k2".to_vec(), 1, ValueType::Put),
                b"v2".to_vec(),
            ),
        ];
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.sort_key(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        for (ik, value) in &entries {
            assert_eq!(reader.get(&ik.sort_key()), Some(value.as_slice()));
        }
    }
}
