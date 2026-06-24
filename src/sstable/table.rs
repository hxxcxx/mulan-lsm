//! SSTable 的整体组装（Builder）与读取（Reader）。
//!
//! 文件布局：
//! ```text
//! [data block 0][data block 1]...[data block N-1]
//! [filter block]      ← 布隆过滤器位数组
//! [metaindex block]   ← Block 格式，存 (key="filter.mulan.BloomFilter", value=filter_handle)
//! [index block]       ← 每 data block 一条 (最大 key, handle)
//! [footer]            ← 固定长度，含 metaindex/index handle + magic
//! ```
//!
//! data block / index block / metaindex block 都用 Block 格式（纯字节 + 前缀压缩）。
//! key 存的是 InternalKey 的 encode 字节（user_key + seq 小端 + type），
//! Block 的 get/lower_bound 用比较器闭包（按 InternalKey Ord 比较），不依赖字节字典序。

use crate::bloom::BloomFilter;
use crate::error::{MulanError, Result};
use crate::internal_key::{
    internal_key_cmp, lookup_key, user_key_of_internal_key, vtype_of_internal_key, ValueType,
};
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
    /// 在线布隆：add 时直接插入，finish 时输出。无需收集全部 key，省内存。
    bloom: BloomFilter,
}

/// metaindex block 中 filter 条目的 key。
const FILTER_META_KEY: &[u8] = b"filter.mulan.BloomFilter";

impl TableBuilder {
    pub fn new(file: std::fs::File) -> Self {
        Self::with_options(file, DATA_BLOCK_TARGET, 10)
    }

    pub fn with_block_target(file: std::fs::File, target: usize) -> Self {
        Self::with_options(file, target, 10)
    }

    pub fn with_options(file: std::fs::File, block_target: usize, bits_per_key: usize) -> Self {
        // 在线布隆预分配：按 block_target 估算 SSTable 约容纳 50 个 data block、
        // 每 block ~block_target/32 条 entry（保守估计 key+value 均长 32B）。
        let estimated_keys = (50 * block_target / 32).max(1000);
        let mut bloom = BloomFilter::new(bits_per_key);
        bloom.ensure_capacity(estimated_keys, bits_per_key);
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
            bloom,
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
        self.bloom.insert(user_key);
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
        if self.finished {
            return Err(MulanError::Corrupted(
                "TableBuilder already finished".into(),
            ));
        }
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

        // 在线布隆：add 时已逐条插入，此处直接序列化输出。
        let bloom_bytes = std::mem::take(&mut self.bloom).finish();
        let filter_handle = self.write_raw(&bloom_bytes)?;
        let mut metaindex_builder = BlockBuilder::new();
        let mut meta_value = Vec::new();
        filter_handle.encode(&mut meta_value);
        metaindex_builder.add(FILTER_META_KEY, &meta_value);
        let metaindex_handle = self.write_raw(&metaindex_builder.finish())?;

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

    /// 当前已写到文件的字节 + 当前 data block 的估计大小。
    /// 用于 compaction 判断是否该切输出文件。
    pub fn current_size_estimate(&self) -> u64 {
        self.file_offset + self.data_block.current_size_estimate() as u64
    }
}

/// SSTable 读取器。打开文件后支持 get(user_key)。
pub struct TableReader {
    /// 整个文件读进内存。大 SSTable 可改为按需读 block（性能优化）。
    data: Vec<u8>,
    index_handle: BlockHandle,
    /// 布隆过滤器，读 data block 前先过滤。
    bloom: Option<BloomFilter>,
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
        // 解析 metaindex block，找到 filter 条目 → 读 filter block 解析布隆。
        // 无 metaindex 或无 filter 条目时 bloom=None，查询降级（全扫 data block 不走布隆）。
        let bloom = (|| -> Option<BloomFilter> {
            let meta_start = metaindex_handle.offset as usize;
            let meta_end = meta_start + metaindex_handle.size as usize;
            let meta_block = Block::new(data.get(meta_start..meta_end)?).ok()?;
            let filter_handle_bytes = meta_block.get(FILTER_META_KEY, &|a, b| a.cmp(b))?;
            let (filter_handle, _) = BlockHandle::decode(filter_handle_bytes).ok()?;
            let fb_start = filter_handle.offset as usize;
            let fb_end = fb_start + filter_handle.size as usize;
            let bloom_bytes = data.get(fb_start..fb_end)?;
            BloomFilter::from_bytes(bloom_bytes)
        })();
        Ok(TableReader {
            data,
            index_handle,
            bloom,
        })
    }

    /// 按 user_key 查找最新版本的 (vtype, value)。命中时返回两者；未命中返回 None。
    /// 流程：布隆过滤 → 哨兵 internal key → index 定位 data block → block 内 lower_bound → 校验 user_key。
    /// 按 user_key 查找 snapshot_seq 时间点的版本。
    /// 流程：布隆过滤 → 哨兵 internal key(seq=snapshot) → index 定位 data block → block 内 lower_bound → 校验 user_key。
    /// 返回 `Ok(None)` 表示 key 不存在；`Err(...)` 表示文件损坏等 I/O 级错误。
    pub fn get_entry(
        &self,
        user_key: &[u8],
        snapshot_seq: u64,
    ) -> Result<Option<(ValueType, &[u8])>> {
        // 布隆过滤：user_key 肯定不在则直接返回 None，省掉读 data block。
        if let Some(bloom) = &self.bloom {
            if !bloom.may_contain(user_key) {
                return Ok(None);
            }
        }
        // 哨兵：user_key + snapshot_seq。同 user_key 下命中的第一个 ≥ 哨兵的 entry 即 ≤ snapshot 的最新版本。
        let lookup = lookup_key(user_key, snapshot_seq);
        // index lower_bound 定位 data block（用 internal_key_cmp 比较）。
        let index_bytes = self.block_bytes(&self.index_handle)?;
        let index_block = Block::new(index_bytes)?;
        let Some(data_handle_bytes) =
            index_block.lower_bound(&lookup, &|a, b| internal_key_cmp(a, b))
        else {
            return Ok(None);
        };
        let (data_handle, _n) = BlockHandle::decode(data_handle_bytes)?;
        // data block 内 lower_bound，校验命中 entry 的 user_key 一致。
        let data_block_bytes = self.block_bytes(&data_handle)?;
        let data_block = Block::new(data_block_bytes)?;
        let Some((found_key, value)) =
            data_block.lower_bound_kv(&lookup, &|a, b| internal_key_cmp(a, b))
        else {
            return Ok(None);
        };
        if user_key_of_internal_key(&found_key) == user_key {
            let vtype = vtype_of_internal_key(&found_key)
                .ok_or_else(|| MulanError::Corrupted("invalid vtype in sstable".into()))?;
            Ok(Some((vtype, value)))
        } else {
            Ok(None)
        }
    }

    /// 按 user_key 查找最新版本。删除标记视为不存在（返回 `Ok(None)`）。
    pub fn get(&self, user_key: &[u8]) -> Result<Option<&[u8]>> {
        match self.get_entry(user_key, crate::internal_key::MAX_SEQUENCE)? {
            Some((ValueType::Put, value)) => Ok(Some(value)),
            Some((ValueType::Delete, _)) | None => Ok(None),
        }
    }

    /// 顺序遍历全表所有 (internal_key, value)，按 `internal_key_cmp` 升序。
    /// 消费 self（move 文件字节进 TableIter），返回 `'static` 的惰性迭代器。
    /// 供 compaction 归并用——可直接 `Box::new(reader.into_table_iter()?)` 喂给 `MergingIterator`，
    /// 避免先 collect 成 Vec 的全量加载。
    pub fn into_table_iter(self) -> Result<TableIter> {
        TableIter::new(self.data, self.index_handle)
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

/// SSTable 顺序迭代器：流式输出全表 (internal_key_bytes, value_bytes)。
///
/// 自持文件全部字节（`'static`），可直接 `Box` 成 `dyn LsmIterator` 喂给 `MergingIterator`。
/// 惰性加载：构造时只解析 index 得到 data block handle 列表，**不解析任何 data block**；
/// `next` 推进时按需解析当前 block 的全部 entry 到 owned `current`，用完再切下一个。
/// 故峰值内存 ≈ 一个 data block（~4KB）而非整表——compaction 读 N 个文件时，
/// 峰值 ≈ N 个 block，而非 N 个整表（避免几十/几百 MB 的全量加载）。
pub struct TableIter {
    data: Vec<u8>,
    handles: Vec<BlockHandle>,
    handle_idx: usize,
    /// 当前 data block 的所有 entry（已解析为 owned）。
    current: Vec<(Vec<u8>, Vec<u8>)>,
    current_pos: usize,
}

impl TableIter {
    /// 从文件全部字节 + index handle 构造。解析 index 得到 data block handle 列表，
    /// 但不解析任何 data block（惰性）。
    fn new(data: Vec<u8>, index_handle: BlockHandle) -> Result<Self> {
        let handles = Self::parse_data_handles(&data, &index_handle).unwrap_or_default();
        Ok(TableIter {
            data,
            handles,
            handle_idx: 0,
            current: Vec::new(),
            current_pos: 0,
        })
    }

    fn parse_data_handles(data: &[u8], index_handle: &BlockHandle) -> Result<Vec<BlockHandle>> {
        let start = index_handle.offset as usize;
        let end = start + index_handle.size as usize;
        let index_bytes = data.get(start..end).ok_or_else(|| {
            MulanError::Corrupted(format!(
                "index handle out of bounds: offset={} size={}",
                index_handle.offset, index_handle.size
            ))
        })?;
        let index_block = Block::new(index_bytes)?;
        let mut handles = Vec::new();
        for (_key, handle_bytes) in index_block.iter() {
            let (h, _n) = BlockHandle::decode(handle_bytes)?;
            handles.push(h);
        }
        Ok(handles)
    }

    /// 已加载（解析）过的 data block 数。供测试观察惰性：构造时应为 0，
    /// 每次 next 跨越 block 边界时 +1。
    pub fn blocks_loaded(&self) -> usize {
        self.handle_idx
    }

    /// data block 总数（来自 index）。供测试判断"是否还有未加载的 block"。
    pub fn blocks_loaded_capacity(&self) -> usize {
        self.handles.len()
    }

    /// 解析下一个 data block 的全部 entry 到 `current`。返回 false 表示已无 block。
    fn advance_to_next_block(&mut self) -> bool {
        while self.handle_idx < self.handles.len() {
            let handle = self.handles[self.handle_idx];
            self.handle_idx += 1;
            let start = handle.offset as usize;
            let end = start + handle.size as usize;
            if let Some(bytes) = self.data.get(start..end) {
                if let Ok(block) = Block::new(bytes) {
                    self.current = block.iter().map(|(k, v)| (k, v.to_vec())).collect();
                    self.current_pos = 0;
                    return true;
                }
            }
            // block 损坏：跳过该 block 继续下一个（容错，与 LevelDB 一致）。
        }
        self.current.clear();
        false
    }

    /// 推进到下一条，返回 owned (key, value)。`Iterator::next` 与 `LsmIterator::next` 共用此逻辑。
    fn next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        loop {
            if self.current_pos < self.current.len() {
                let item = self.current[self.current_pos].clone();
                self.current_pos += 1;
                return Some(item);
            }
            // 当前 block 耗尽，切下一个。
            if !self.advance_to_next_block() {
                return None;
            }
        }
    }
}

impl Iterator for TableIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        self.next_entry()
    }
}

impl crate::iterator::LsmIterator for TableIter {
    fn peek(&self) -> Option<(&[u8], &[u8])> {
        self.current
            .get(self.current_pos)
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        self.next_entry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_key::{internal_key_cmp, InternalKey, ValueType};
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
                reader.get(user_key).unwrap(),
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
            assert_eq!(reader.get(user_key).unwrap(), Some(value.as_slice()));
        }
        // 未命中：不存在的 user_key（布隆应拦截）。
        let missing_user_key = (u32::MAX).to_be_bytes();
        assert_eq!(reader.get(&missing_user_key).unwrap(), None);
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
        assert_eq!(reader.get(b"zzz-not-exist").unwrap(), None);
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
        assert_eq!(reader.get(b"k1").unwrap(), None);
        assert_eq!(
            reader
                .get_entry(b"k1", crate::internal_key::MAX_SEQUENCE)
                .unwrap()
                .map(|(t, v)| (t, v.len())),
            Some((ValueType::Delete, 0))
        );
        // k2 最新是 Put v2。
        assert_eq!(reader.get(b"k2").unwrap(), Some(b"v2".as_slice()));
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
                reader.get(&ik.user_key).unwrap(),
                Some(value.as_slice()),
                "present key missed: {:?}",
                ik.user_key
            );
        }
        // 不存在的 key 全部返回 None。
        for i in 0..500u32 {
            let absent = format!("absent-{i}");
            assert_eq!(reader.get(absent.as_bytes()).unwrap(), None);
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
            assert_eq!(reader.get(&ik.user_key).unwrap(), Some(value.as_slice()));
        }
        // 不存在的 key 全部返回 None。
        for i in 0..500u32 {
            let absent = format!("absent-{i}");
            assert_eq!(reader.get(absent.as_bytes()).unwrap(), None);
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
        assert_eq!(reader.get(b"key1").unwrap(), Some(b"v1-updated".as_slice()));
        // key2 最新是删除标记：get 返回 None，get_entry 暴露 (Delete, 空)。
        assert_eq!(reader.get(b"key2").unwrap(), None);
        assert_eq!(
            reader
                .get_entry(b"key2", crate::internal_key::MAX_SEQUENCE)
                .unwrap()
                .map(|(t, v)| (t, v.len())),
            Some((ValueType::Delete, 0))
        );
        // key3 正常。
        assert_eq!(reader.get(b"key3").unwrap(), Some(b"v3".as_slice()));
        // 不存在的 key。
        assert_eq!(reader.get(b"key4").unwrap(), None);
    }

    #[test]
    fn table_iter_empty_table_ends_immediately() {
        // 空 SSTable（无 entry）的迭代器应立即结束。
        let path = tmp_path("iter_empty.sst");
        let file = std::fs::File::create(&path).unwrap();
        TableBuilder::new(file).finish().unwrap();
        let reader = TableReader::open(&path).unwrap();
        let collected: Vec<_> = reader.into_table_iter().unwrap().collect();
        assert!(collected.is_empty());
    }

    #[test]
    fn table_iter_outputs_all_entries_in_order() {
        // 单 data block：迭代器输出全部 entry，按 internal_key_cmp 升序。
        let path = tmp_path("iter_small.sst");
        let entries = ordered_entries(20, 10);
        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let collected: Vec<(Vec<u8>, Vec<u8>)> = reader.into_table_iter().unwrap().collect();
        assert_eq!(collected.len(), entries.len());
        // 字节与写入一致（internal key 字节 + value）。
        for (i, ((ik, value), (got_key, got_val))) in
            entries.iter().zip(collected.iter()).enumerate()
        {
            assert_eq!(got_key, &ik.encode(), "key mismatch at {i}");
            assert_eq!(got_val, value, "value mismatch at {i}");
        }
    }

    #[test]
    fn table_iter_crosses_data_block_boundary() {
        // 多 data block：迭代器跨 block 连续输出，无遗漏/重复。
        let path = tmp_path("iter_multi.sst");
        let entries = ordered_entries(5_000, 1_000);
        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();
        assert!(std::fs::metadata(&path).unwrap().len() > DATA_BLOCK_TARGET as u64);

        let reader = TableReader::open(&path).unwrap();
        let collected: Vec<_> = reader.into_table_iter().unwrap().collect();
        assert_eq!(
            collected.len(),
            entries.len(),
            "no entries lost across blocks"
        );
        // 严格按 internal_key_cmp 升序。
        for w in collected.windows(2) {
            assert!(
                internal_key_cmp(&w[0].0, &w[1].0) != std::cmp::Ordering::Greater,
                "entries not in ascending order"
            );
        }
    }

    #[test]
    fn table_iter_preserves_delete_and_multi_version() {
        // 迭代器输出所有版本（含 Delete 标记和旧版本），不做去重——去重是 MergingIterator 的职责。
        let path = tmp_path("iter_versions.sst");
        let mut entries = vec![
            (
                InternalKey::new(b"k".to_vec(), 1, ValueType::Put),
                b"v1".to_vec(),
            ),
            (
                InternalKey::new(b"k".to_vec(), 2, ValueType::Delete),
                Vec::new(),
            ),
            (
                InternalKey::new(b"k".to_vec(), 3, ValueType::Put),
                b"v3".to_vec(),
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
        let collected: Vec<_> = reader.into_table_iter().unwrap().collect();
        // 3 条全输出（不去重）。
        assert_eq!(collected.len(), 3);
        // Ord 升序：seq=3 在前（大 seq 排前），seq=2 中，seq=1 后。
        let seqs: Vec<u64> = collected
            .iter()
            .map(|(k, _)| InternalKey::decode(k).unwrap().seq)
            .collect();
        assert_eq!(seqs, vec![3, 2, 1]);
        // Delete 标记的 entry 也在（value 空）。
        assert_eq!(collected[1].1, b"");
    }

    /// 惰性加载验证：TableIter 构造时不解析任何 data block，next 跨 block 边界时才加载。
    ///
    /// 对比改前：`reader.iter().collect::<Vec<_>>()` 会驱动迭代器到耗尽，
    /// 一次性解析全部 data block，峰值内存 = 整表所有 entry。
    /// 改后 `into_iter()` 返回惰性 TableIter，按需加载一个 block，
    /// 峰值内存 ≈ 一个 block（~4KB）。本测试用 blocks_loaded() 观察这个差异。
    #[test]
    fn table_iter_is_lazy() {
        use crate::iterator::LsmIterator;
        let path = tmp_path("lazy.sst");
        // 构造一个大 SSTable：5000 条，block target 调小（256B）强制切成多个 data block。
        let mut entries: Vec<(InternalKey, Vec<u8>)> = (0..5000u32)
            .map(|i| {
                (
                    InternalKey::new((i as u64).to_be_bytes().to_vec(), 1, ValueType::Put),
                    format!("value-{i}").into_bytes(),
                )
            })
            .collect();
        entries.sort_by(|(a, _), (b, _)| a.cmp(b));

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::with_block_target(file, 256);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        let reader = TableReader::open(&path).unwrap();
        let mut iter = reader.into_table_iter().unwrap();
        let total_blocks = iter.blocks_loaded_capacity();
        assert!(total_blocks > 5, "测试需要多个 block，实际 {total_blocks}");

        // 1. 构造 TableIter 后、未 next 前：blocks_loaded == 0（没解析任何 data block）。
        assert_eq!(iter.blocks_loaded(), 0, "构造时不该加载任何 data block");

        // 2. 取前 3 条：只加载了第 1 个 block（blocks_loaded == 1）。
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(Iterator::next(&mut iter).unwrap());
        }
        assert_eq!(got.len(), 3);
        assert_eq!(
            iter.blocks_loaded(),
            1,
            "取 3 条只该加载 1 个 block，实际 {}",
            iter.blocks_loaded()
        );

        // 3. 提前 drop：剩余 block 从未被加载。blocks_loaded 远小于总数。
        let loaded_so_far = iter.blocks_loaded();
        drop(iter);
        assert!(
            loaded_so_far < total_blocks,
            "提前终止时只加载 {loaded_so_far} 个 block，但总共 {total_blocks}——\
             若全量加载则违背惰性"
        );

        // 4. 完整遍历应加载全部 block（验证惰性不丢数据）。
        let reader2 = TableReader::open(&path).unwrap();
        let mut iter2: Box<dyn LsmIterator> = Box::new(reader2.into_table_iter().unwrap());
        let mut count = 0;
        while iter2.next().is_some() {
            count += 1;
        }
        assert_eq!(count, 5000, "完整遍历必须吐出全部 5000 条");
    }

    /// 损坏 SSTable（让 index block 的 num_restarts 超大数据）→ get_entry 返回 Err 而非 Ok(None)。
    /// 这是 P0 修改的核心语义：数据损坏必须向上传播，不能静默当作"key 不存在"。
    #[test]
    fn corrupted_index_returns_error_not_none() {
        let path = tmp_path("corrupt_idx.sst");
        let entries = ordered_entries(30, 15);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        // 正常读：get_entry 返回 Ok(Some(...))
        let reader = TableReader::open(&path).unwrap();
        let first_key = &entries[0].0.user_key;
        assert!(reader
            .get_entry(first_key, crate::internal_key::MAX_SEQUENCE)
            .unwrap()
            .is_some());

        // 损坏 index block 的 num_restarts：index block 紧贴 footer 之前，
        // 其末尾 4 字节即 num_restarts。改写为超大值令 Block::new 拒绝。
        let mut data = std::fs::read(&path).unwrap();
        let num_restarts_offset = data.len() - FOOTER_LEN - 4;
        // 写入 0x3FFF_FFFF → restarts_bytes = 4GB → 远超文件大小 → Block::new 返回 Err
        data[num_restarts_offset..num_restarts_offset + 4]
            .copy_from_slice(&0x3FFF_FFFFu32.to_le_bytes());
        std::fs::write(&path, &data).unwrap();

        // 损坏后：get_entry 必须返回 Err
        let reader = TableReader::open(&path).unwrap();
        let result = reader.get_entry(first_key, crate::internal_key::MAX_SEQUENCE);
        assert!(
            result.is_err(),
            "corrupted index block must produce Err, got {result:?}"
        );
    }

    /// 损坏 SSTable（block handle 指向越界）→ get_entry 返回 Err。
    #[test]
    fn out_of_bounds_block_handle_returns_error() {
        let path = tmp_path("corrupt_handle.sst");
        let entries = ordered_entries(10, 5);

        let file = std::fs::File::create(&path).unwrap();
        let mut builder = TableBuilder::new(file);
        for (ik, value) in &entries {
            builder.add(&ik.user_key, &ik.encode(), value).unwrap();
        }
        builder.finish().unwrap();

        // 读取并修改 index handle 的 offset 为超大的值
        let mut data = std::fs::read(&path).unwrap();
        let footer = &data[data.len() - FOOTER_LEN..];
        // footer 布局: [metaindex_handle 变长] [index_handle 变长] [padding] [magic 8字节]
        let (metaindex_handle, n1) = BlockHandle::decode(footer).unwrap();
        let (_index_handle, _n2) = BlockHandle::decode(&footer[n1..]).unwrap();
        // 重写 index handle offset 为超大值（超出文件长度）
        let mut new_footer = Vec::new();
        metaindex_handle.encode(&mut new_footer);
        let bogus_handle = BlockHandle {
            offset: data.len() as u64 + 100_000,
            size: 100,
        };
        bogus_handle.encode(&mut new_footer);
        // 补 padding + magic
        let _used = new_footer.len();
        new_footer.resize(FOOTER_LEN - 8, 0);
        new_footer.extend_from_slice(&footer[footer.len() - 8..]);

        let footer_start = data.len() - FOOTER_LEN;
        data[footer_start..].copy_from_slice(&new_footer);

        std::fs::write(&path, &data).unwrap();

        let reader = TableReader::open(&path).unwrap();
        let result = reader.get_entry(&entries[0].0.user_key, crate::internal_key::MAX_SEQUENCE);
        assert!(
            result.is_err(),
            "out-of-bounds block handle must produce Err, got {result:?}"
        );
    }
}
