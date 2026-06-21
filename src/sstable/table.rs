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

/// SSTable 构造器。按 key 有序（InternalKey Ord）追加 kv，自动切 data block、维护 index。
/// key 存 internal key 的 encode 字节（user_key+seq 小端+type），查找用 internal_key_cmp。
pub struct TableBuilder {
    file: std::fs::File,
    /// 当前正在累积的 data block。
    data_block: BlockBuilder,
    /// index block：每条 (上一个 data block 的最大 key, 该 block 的 handle)。
    index_block: BlockBuilder,
    /// 当前已写入文件的字节数（= 下个 block 的起始 offset）。
    file_offset: u64,
    /// 最近一次 add 的 internal key（即当前 data block 的最大 key）。
    last_key: Vec<u8>,
    /// 上一个 data block 已落盘但 index 项还没写入：记下它的最大 key 和 handle。
    pending_index_key: Option<Vec<u8>>,
    pending_handle: Option<BlockHandle>,
    /// 已写入的 entry 数。
    num_entries: u64,
    finished: bool,
    /// data block 切分阈值。测试时可调小以精确控制 block 数。
    block_target: usize,
    /// 收集所有 user_key，finish 时构建布隆。
    user_keys: Vec<Vec<u8>>,
    /// 布隆的 bits_per_key。
    bits_per_key: usize,
}

impl TableBuilder {
    pub fn new(file: std::fs::File) -> Self {
        Self::with_options(file, DATA_BLOCK_TARGET, 10)
    }

    pub fn with_block_target(file: std::fs::File, target: usize) -> Self {
        Self::with_options(file, target, 10)
    }

    pub fn with_options(file: std::fs::File, block_target: usize, bits_per_key: usize) -> Self {
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
            block_target,
            user_keys: Vec::new(),
            bits_per_key,
        }
    }

    /// 追加一条 (user_key, internal_key_bytes, value)。
    /// internal_key_bytes 是 InternalKey::encode() 的字节，必须按 InternalKey Ord 严格升序。
    /// user_key 用于构建布隆（查询时按 user_key 过滤）。
    pub fn add(&mut self, user_key: &[u8], internal_key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!self.finished, "TableBuilder already finished");

        // 若上一个 data block 已切出但 index 项还没记录，现在用它的最大 key 补上。
        if let Some(key) = self.pending_index_key.take() {
            if let Some(handle) = self.pending_handle.take() {
                let mut handle_buf = Vec::new();
                handle.encode(&mut handle_buf);
                self.index_block.add(&key, &handle_buf);
            }
        }

        self.data_block.add(internal_key, value);
        self.last_key.clear();
        self.last_key.extend_from_slice(internal_key);
        self.user_keys.push(user_key.to_vec());
        self.num_entries += 1;

        // data block 攒够大小，切出并写盘。
        if self.data_block.current_size_estimate() >= self.block_target {
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

        // 构建 Bloom Filter 写入 meta block（直接用布隆字节作为 meta block 内容）。
        let bloom_keys: Vec<&[u8]> = self.user_keys.iter().map(|v| v.as_slice()).collect();
        let bloom = crate::bloom::BloomFilter::from_keys(&bloom_keys, self.bits_per_key);
        let metaindex_bytes = bloom.finish();
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

/// SSTable 读取器。打开文件后支持 get(user_key)。
pub struct TableReader {
    /// 整个文件读进内存。第一版简化（大 SSTable 可改为按需读 block）。
    data: Vec<u8>,
    index_handle: BlockHandle,
    /// 布隆过滤器，读 data block 前先过滤。
    bloom: Option<crate::bloom::BloomFilter>,
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
        let (metaindex_handle, n1) = BlockHandle::decode(footer)?;
        let (index_handle, _n2) = BlockHandle::decode(&footer[n1..])?;
        // 解析布隆（meta block 内容直接是布隆字节）。
        let bloom_start = metaindex_handle.offset as usize;
        let bloom_end = bloom_start + metaindex_handle.size as usize;
        let bloom = if bloom_end <= data.len() {
            crate::bloom::BloomFilter::from_bytes(&data[bloom_start..bloom_end])
        } else {
            None
        };
        Ok(TableReader {
            data,
            index_handle,
            bloom,
        })
    }

    /// 按 user_key 查找最新版本的 (vtype, value)。命中时返回两者；未命中返回 None。
    /// 流程：布隆过滤 → 哨兵 internal key → index 定位 data block → block 内 lower_bound → 校验 user_key。
    pub fn get_entry(&self, user_key: &[u8]) -> Option<(crate::internal_key::ValueType, &[u8])> {
        use crate::internal_key::{
            internal_key_cmp, lookup_key, user_key_of_internal_key, vtype_of_internal_key,
        };
        // 布隆过滤：user_key 肯定不在则直接返回 None，省掉读 data block。
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(user_key) {
                return None;
            }
        }
        // 构造哨兵 internal key：user_key + MAX_SEQUENCE。同 user_key 下 Ord 最小（排最前），
        // lower_bound 命中的第一个 >= 哨兵的 entry 即最新版本。
        let lookup = lookup_key(user_key);
        // index lower_bound 定位 data block（用 internal_key_cmp 比较）。
        let index_bytes = self.block_bytes(&self.index_handle).ok()?;
        let index_block = Block::new(index_bytes).ok()?;
        let data_handle_bytes = index_block.lower_bound(&lookup, &|a, b| internal_key_cmp(a, b))?;
        let (data_handle, _n) = BlockHandle::decode(data_handle_bytes).ok()?;
        // data block 内 lower_bound，校验命中 entry 的 user_key 一致。
        let data_block_bytes = self.block_bytes(&data_handle).ok()?;
        let data_block = Block::new(data_block_bytes).ok()?;
        let (found_key, value) =
            data_block.lower_bound_kv(&lookup, &|a, b| internal_key_cmp(a, b))?;
        if user_key_of_internal_key(&found_key) == user_key {
            Some((vtype_of_internal_key(&found_key), value))
        } else {
            None
        }
    }

    /// 按 user_key 查找最新版本。删除标记视为不存在（返回 None）。
    pub fn get(&self, user_key: &[u8]) -> Option<&[u8]> {
        let (vtype, value) = self.get_entry(user_key)?;
        if vtype == crate::internal_key::ValueType::Delete {
            None
        } else {
            Some(value)
        }
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
    use crate::internal_key::{InternalKey, ValueType};
    use std::path::PathBuf;

    static DIR_COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

    fn tmp_path(name: &str) -> PathBuf {
        let n = DIR_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("mulan-sstable-test-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join(name)
    }

    /// 构造一组有序 InternalKey（按 Ord 升序），返回 (InternalKey, value) 对。
    fn ordered_entries(count: u32, key_space: u32) -> Vec<(InternalKey, Vec<u8>)> {
        let mut entries: Vec<(InternalKey, Vec<u8>)> = Vec::new();
        // 每个 user_key 写多个版本（seq 递增），保证多版本在 sort_key 下正确排列。
        for i in 0..count {
            let user_key = (i % key_space).to_be_bytes().to_vec();
            let seq = (i / key_space + 1) as u64;
            let ik = InternalKey::new(user_key, seq, ValueType::Put);
            entries.push((ik, format!("v{i}").into_bytes()));
        }
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));
        entries
    }

    /// 计算每个 user_key 的最新版本（最大 seq）的 value，作为 get 的期望结果。
    fn latest_value_per_user_key(
        entries: &[(InternalKey, Vec<u8>)],
    ) -> std::collections::HashMap<Vec<u8>, Vec<u8>> {
        let mut map = std::collections::HashMap::new();
        for (ik, value) in entries {
            // entries 已按 Ord 升序，同 user_key 下大 seq 在前，故第一条即最新；
            // 但为稳健起见，取 seq 最大的。
            map.entry(ik.user_key.clone())
                .and_modify(|existing: &mut (u64, Vec<u8>)| {
                    if ik.seq > existing.0 {
                        *existing = (ik.seq, value.clone());
                    }
                })
                .or_insert_with(|| (ik.seq, value.clone()));
        }
        map.into_iter().map(|(k, (_, v))| (k, v)).collect()
    }

    #[test]
    fn round_trip_small_table() {
        let path = tmp_path("small.sst");
        let entries = ordered_entries(20, 10);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        assert_eq!(builder.num_entries(), 20);
        builder.finish().unwrap();

        // 读回：get(user_key) 返回该 user_key 的最新版本。
        let reader = TableReader::open(&path).unwrap();
        let expected = latest_value_per_user_key(&entries);
        for (user_key, value) in &expected {
            assert_eq!(
                reader.get(user_key),
                Some(value.as_slice()),
                "missed user_key {user_key:?}"
            );
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
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > DATA_BLOCK_TARGET as u64);

        let reader = TableReader::open(&path).unwrap();
        let expected = latest_value_per_user_key(&entries);
        for (user_key, value) in &expected {
            assert_eq!(reader.get(user_key), Some(value.as_slice()));
        }
        // 未命中：不存在的 user_key（布隆应拦截）。
        let missing_user_key = (u32::MAX).to_be_bytes();
        assert_eq!(reader.get(&missing_user_key), None);
    }

    #[test]
    fn get_missing_key_returns_none() {
        let path = tmp_path("missing.sst");
        let entries = ordered_entries(5, 5);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        // 完全不存在的 user_key。
        assert_eq!(reader.get(b"zzz-not-exist"), None);
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
        let mut buf = vec![0u8; FOOTER_LEN];
        buf.extend_from_slice(&0xDEAD_BEEFu64.to_le_bytes());
        std::fs::write(&path, &buf).unwrap();
        assert!(TableReader::open(&path).is_err());
    }

    #[test]
    fn delete_entries_stored_and_found() {
        // Delete 类型也能存进 SSTable（value 空）。
        // get 返回最新版本：k1 最新是 Delete，但 SSTable 层不解释 Delete 语义，
        // 它返回该 entry 的 value（空切片）。Delete 的解释（视为不存在）由上层 DB 做。
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
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        // k1 最新版本是 Delete：get 返回 None（删除标记由上层解释为不存在），
        // get_entry 暴露 (Delete, 空) 以证明标记确实被存储。
        assert_eq!(reader.get(b"k1"), None);
        assert_eq!(
            reader.get_entry(b"k1").map(|(t, v)| (t, v.len())),
            Some((ValueType::Delete, 0))
        );
        // k2 最新是 Put v2。
        assert_eq!(reader.get(b"k2"), Some(b"v2".as_slice()));
    }

    #[test]
    fn variable_length_user_keys_correct() {
        // 变长 user_key + 前缀关系（如 "present-2" 是 "present-200" 前缀）。
        // 这是纯字节 Block + internal key 的关键考验：前缀压缩 + 查找都不能跨 user_key/seq 边界出错。
        let path = tmp_path("varlen.sst");
        let mut entries: Vec<(InternalKey, Vec<u8>)> = (0..500u32)
            .map(|i| {
                (
                    InternalKey::new(format!("present-{i}").into_bytes(), 1, ValueType::Put),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        // 必须按 InternalKey Ord 升序喂给 builder（约束 #3）。
        // 数值序（present-0,1,2,...,10）≠ 字典序（present-1 < present-10 < present-2）。
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        // 存在的 key 全部命中。
        for (ik, value) in &entries {
            assert_eq!(
                reader.get(&ik.user_key),
                Some(value.as_slice()),
                "present key missed: {:?}",
                ik.user_key
            );
        }
        // 不存在的 key 全部返回 None。
        for i in 0..500u32 {
            let absent = format!("absent-{i}");
            assert_eq!(reader.get(absent.as_bytes()), None);
        }
    }

    #[test]
    fn bloom_rejects_absent_user_keys() {
        // 布隆过滤：不存在的 user_key 被布隆拒绝（may_contain=false 直接 None），
        // 即便误判通过，user_key 校验也返回 None。双保险。
        let path = tmp_path("bloom.sst");
        let mut entries: Vec<(InternalKey, Vec<u8>)> = (0..500u32)
            .map(|i| {
                (
                    InternalKey::new(format!("present-{i}").into_bytes(), 1, ValueType::Put),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        // 存在的 key 全部命中。
        for (ik, value) in &entries {
            assert_eq!(reader.get(&ik.user_key), Some(value.as_slice()));
        }
        // 不存在的 key 全部返回 None。
        for i in 0..500u32 {
            let absent = format!("absent-{i}");
            assert_eq!(reader.get(absent.as_bytes()), None);
        }
    }

    #[test]
    fn memtable_flush_to_sstable_round_trip() {
        // 端到端：MemTable 写入 → flush 成 SSTable → TableReader 读回 → get 验证。
        use crate::memtable::MemTable;
        let path = tmp_path("flush.sst");
        let mut memtable = MemTable::new();
        // 多版本 + 删除标记，覆盖 MemTable 的全部语义。
        memtable.put(b"key1", b"v1");
        memtable.put(b"key1", b"v1-updated");
        memtable.put(b"key2", b"v2");
        memtable.delete(b"key2");
        memtable.put(b"key3", b"v3");

        let n = memtable.flush_to_sstable(&path).unwrap();
        assert_eq!(n, 5); // 5 条 entry（含删除标记和多版本）

        let reader = TableReader::open(&path).unwrap();
        // key1 最新版本是 v1-updated。
        assert_eq!(reader.get(b"key1"), Some(b"v1-updated".as_slice()));
        // key2 最新是删除标记：get 返回 None，get_entry 暴露 (Delete, 空)。
        assert_eq!(reader.get(b"key2"), None);
        assert_eq!(
            reader.get_entry(b"key2").map(|(t, v)| (t, v.len())),
            Some((ValueType::Delete, 0))
        );
        // key3 正常。
        assert_eq!(reader.get(b"key3"), Some(b"v3".as_slice()));
        // 不存在的 key。
        assert_eq!(reader.get(b"key4"), None);
    }
}
