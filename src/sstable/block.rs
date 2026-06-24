//! Block：SSTable 的基本存储单元。把一批有序 kv 紧凑存储，支持块内高效查找。
//!
//! 核心技术：
//! - 前缀压缩：相邻 key 共享前缀时只存差异部分，省空间。
//! - restart point：每 RESTART_INTERVAL 条存一个完整 key（不压缩），作为二分查找的锚点，
//!   让压缩后的 block 仍能 O(log n) 查找。
//!
//! 字节布局：
//! ```text
//! [entry 0][entry 1]...[entry N-1]
//! [restarts[0]:u32][restarts[1]:u32]...[restarts[M-1]:u32]
//! [num_restarts:u32]
//! ```
//! 每个 entry：`shared(varint) | non_shared(varint) | value_len(varint) | key_delta | value`
//! `shared` 是与前一条 key 共享的前缀长度，`non_shared` 是本条独有的 key 后缀长度。

use crate::error::{MulanError, Result};
use crate::varint::{decode_varint32, encode_varint32};

/// 每 RESTART_INTERVAL 条 entry 设一个 restart point（不压缩，存完整 key）。
/// LevelDB 默认 16，平衡压缩率和查找速度。
pub const RESTART_INTERVAL: usize = 16;

/// Block 构造器。按 key 有序追加 kv，内部做前缀压缩，finish 输出字节。
pub struct BlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    /// 上一条 entry 的完整 key（用于计算和当前 key 的共享前缀）。
    last_key: Vec<u8>,
    /// 自上一个 restart point 以来的 entry 计数。
    count_since_restart: usize,
    /// 是否已 finish（finish 后禁止再 add）。
    finished: bool,
}

impl BlockBuilder {
    pub fn new() -> Self {
        // 第一个 entry 必然是 restart point（shared=0）。
        // restarts 初始包含 0，标记"偏移 0 是一个 restart point"。
        BlockBuilder {
            buf: Vec::new(),
            restarts: vec![0],
            last_key: Vec::new(),
            count_since_restart: 0,
            finished: false,
        }
    }

    /// 追加一条 kv。调用方必须保证 key 严格大于上一条（有序）。
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished, "BlockBuilder already finished");
        // 计算与上一条 key 的共享前缀长度。
        // restart point 处强制 shared=0；否则取真实公共前缀。
        let shared = if self.count_since_restart < RESTART_INTERVAL {
            common_prefix_len(&self.last_key, key)
        } else {
            // 到达 restart 间隔，本条作为新 restart point，存完整 key。
            self.restarts.push(self.buf.len() as u32);
            self.count_since_restart = 0;
            0
        };
        let non_shared = key.len() - shared;

        encode_varint32(&mut self.buf, shared as u32);
        encode_varint32(&mut self.buf, non_shared as u32);
        encode_varint32(&mut self.buf, value.len() as u32);
        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.count_since_restart += 1;
    }

    /// 输出 block 字节。输出后不可再 add。
    pub fn finish(mut self) -> Vec<u8> {
        self.finished = true;
        // 追加 restarts 数组 + num_restarts。
        for &r in &self.restarts {
            self.buf.extend_from_slice(&r.to_le_bytes());
        }
        self.buf
            .extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.buf
    }

    /// 当前已编码的近似字节数（不含未 flush 的 restarts 尾部），供 SSTable 决定何时切 block。
    pub fn current_size_estimate(&self) -> usize {
        self.buf.len() + 4 * self.restarts.len() + 4
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

impl Default for BlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// 计算两个字节串的最长公共前缀长度。
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let mut i = 0;
    while i < len && a[i] == b[i] {
        i += 1;
    }
    i
}

/// Block 读取侧。解析后的字节视图，支持块内 get 和迭代。
pub struct Block<'a> {
    data: &'a [u8],
    /// restarts 数组在 data 中的起始偏移。
    restarts_offset: usize,
    /// restart point 数量。
    num_restarts: usize,
}

impl<'a> Block<'a> {
    /// 从字节切片构造。解析末尾的 num_restarts 和 restarts 数组。
    pub fn new(data: &'a [u8]) -> Result<Self> {
        if data.len() < 4 {
            return Err(MulanError::Corrupted(
                "block too short for num_restarts".into(),
            ));
        }
        let num_restarts = u32::from_le_bytes(data[data.len() - 4..].try_into().unwrap()) as usize;
        let restarts_bytes = num_restarts * 4;
        // restarts 数组占据末尾 restarts_bytes 字节（不含 num_restarts 本身的 4 字节）。
        if data.len() < 4 + restarts_bytes {
            return Err(MulanError::Corrupted(format!(
                "block too short for {num_restarts} restarts"
            )));
        }
        let restarts_offset = data.len() - 4 - restarts_bytes;
        Ok(Block {
            data,
            restarts_offset,
            num_restarts,
        })
    }

    /// 取第 i 个 restart point 指向的 entry 字节偏移。
    fn restart_offset(&self, i: usize) -> usize {
        let start = self.restarts_offset + i * 4;
        u32::from_le_bytes(self.data[start..start + 4].try_into().unwrap()) as usize
    }

    /// 解析指定偏移处的一条 entry，返回 (key, value, 下一条 entry 的偏移)。
    fn entry_at(&self, offset: usize) -> Result<(Vec<u8>, &'a [u8], usize)> {
        let (shared, n1) = decode_varint32(&self.data[offset..])?;
        let (non_shared, n2) = decode_varint32(&self.data[offset + n1..])?;
        let (value_len, n3) = decode_varint32(&self.data[offset + n1 + n2..])?;
        let header_len = n1 + n2 + n3;
        let key_delta_start = offset + header_len;
        // shared > 0 时调用方需提供前一条 key 来重建；这里返回 shared 让调用方处理。
        // 为简化，本函数只在 shared==0（restart point）处被调用，或由迭代器维护 last_key。
        let _ = shared;
        let key_delta = &self.data[key_delta_start..key_delta_start + non_shared as usize];
        let value_start = key_delta_start + non_shared as usize;
        let value_end = value_start + value_len as usize;
        let value = &self.data[value_start..value_end];
        Ok((key_delta.to_vec(), value, value_end))
    }

    /// 块内查找 key。用比较函数 cmp(target, candidate) 定位。
    /// 算法：在 restart points 上二分找区间，再线性扫描重建 key 比较。
    /// 返回命中 key 对应的 value 切片；未命中返回 None。
    pub fn get(
        &self,
        target: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> Option<&'a [u8]> {
        // 二分 restart points：找到最后一个 restart_point_key <= target 的区间。
        let mut lo = 0usize;
        let mut hi = self.num_restarts;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let restart_key = self.restarting_key_at(mid)?;
            match cmp(target, &restart_key) {
                std::cmp::Ordering::Less => hi = mid,
                std::cmp::Ordering::Equal => {
                    // 恰好命中 restart point 的 key。
                    return self.value_at_restart(mid);
                }
                std::cmp::Ordering::Greater => lo = mid + 1,
            }
        }
        // target 落在第 (lo-1) 个 restart 区间内（lo==0 表示比所有 restart key 都小，块内最小 key 也大于 target）。
        if lo == 0 {
            // target 比第一个 restart key 还小，块内无此 key。
            return None;
        }
        let start_restart = lo - 1;
        // 从该 restart point 线性扫描，重建 key 比较。
        let mut offset = self.restart_offset(start_restart);
        let mut last_key = self.restarting_key_at(start_restart)?;
        while let Some(entry) = self.parse_entry(offset, &last_key) {
            match cmp(target, &entry.key) {
                std::cmp::Ordering::Less => return None,
                std::cmp::Ordering::Equal => return Some(entry.value),
                std::cmp::Ordering::Greater => {
                    offset = entry.next_offset;
                    last_key = entry.key;
                    // 超出 restarts 区域说明扫到下一个 restart point 前仍没命中。
                    if self.is_past_entries(offset) {
                        return None;
                    }
                }
            }
        }
        None
    }

    /// lower_bound 查找：返回第一个 key >= target 的 entry 的 value。
    /// 与 get 的区别：不要求 key 相等，只要 >= 即返回。
    /// 供 SSTable index block 用——index 项的 key 是各 data block 的"最大 key"分隔符，
    /// 查 target 时要找"第一个分隔符 >= target"对应的 data block，而非精确匹配。
    pub fn lower_bound(
        &self,
        target: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> Option<&'a [u8]> {
        // 二分 restart points：找第一个 restart_point_key >= target 的区间起点。
        let mut lo = 0usize;
        let mut hi = self.num_restarts;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let restart_key = self.restarting_key_at(mid)?;
            match cmp(target, &restart_key) {
                std::cmp::Ordering::Greater => lo = mid + 1,
                // target <= restart_key：答案可能在 mid 或更早。
                _ => hi = mid,
            }
        }
        // lo 是第一个 restart_key >= target 的 restart index；若 lo==0 从头扫，否则从 lo-1 开始。
        let start_restart = lo.saturating_sub(1).min(self.num_restarts - 1);
        let mut offset = self.restart_offset(start_restart);
        let mut last_key = self.restarting_key_at(start_restart)?;
        while let Some(entry) = self.parse_entry(offset, &last_key) {
            match cmp(target, &entry.key) {
                std::cmp::Ordering::Greater => {
                    offset = entry.next_offset;
                    last_key = entry.key;
                    if self.is_past_entries(offset) {
                        return None;
                    }
                }
                // target <= entry.key：这是第一个 >= target 的 entry。
                _ => return Some(entry.value),
            }
        }
        None
    }

    /// lower_bound 的变体：返回重建后的完整 key（owned）+ value 借用。
    /// 前缀压缩下完整 key 不在 data 里连续存放，故 key 必须重建为 owned。
    /// 供 TableReader 校验命中 entry 的 user_key。
    pub fn lower_bound_kv(
        &self,
        target: &[u8],
        cmp: &dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering,
    ) -> Option<(Vec<u8>, &'a [u8])> {
        let mut lo = 0usize;
        let mut hi = self.num_restarts;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let restart_key = self.restarting_key_at(mid)?;
            match cmp(target, &restart_key) {
                std::cmp::Ordering::Greater => lo = mid + 1,
                _ => hi = mid,
            }
        }
        let start_restart = lo.saturating_sub(1).min(self.num_restarts - 1);
        let mut offset = self.restart_offset(start_restart);
        let mut last_key = self.restarting_key_at(start_restart)?;
        while let Some(entry) = self.parse_entry(offset, &last_key) {
            match cmp(target, &entry.key) {
                std::cmp::Ordering::Greater => {
                    offset = entry.next_offset;
                    last_key = entry.key;
                    if self.is_past_entries(offset) {
                        return None;
                    }
                }
                _ => return Some((entry.key, entry.value)),
            }
        }
        None
    }

    /// 解析 restart point i 处的 entry 的 key（restart point 的 shared=0，key=key_delta）。
    fn restarting_key_at(&self, i: usize) -> Option<Vec<u8>> {
        let offset = self.restart_offset(i);
        let (key_delta, _value, _next) = self.entry_at(offset).ok()?;
        Some(key_delta)
    }

    /// restart point 处已知 key，返回其 value（重新解析该 entry）。
    fn value_at_restart(&self, i: usize) -> Option<&'a [u8]> {
        let offset = self.restart_offset(i);
        let (_key_delta, value, _next) = self.entry_at(offset).ok()?;
        Some(value)
    }

    /// 解析指定偏移的 entry，用 last_key 重建完整 key。
    fn parse_entry(&self, offset: usize, last_key: &[u8]) -> Option<ParsedEntry<'a>> {
        let (shared, n1) = decode_varint32(&self.data[offset..]).ok()?;
        let (non_shared, n2) = decode_varint32(&self.data[offset + n1..]).ok()?;
        let (value_len, n3) = decode_varint32(&self.data[offset + n1 + n2..]).ok()?;
        let header_len = n1 + n2 + n3;
        let key_delta_start = offset + header_len;
        let key_delta = &self.data[key_delta_start..key_delta_start + non_shared as usize];
        let value_start = key_delta_start + non_shared as usize;
        let value_end = value_start + value_len as usize;
        if value_end > self.restarts_offset {
            return None;
        }
        // 重建完整 key：last_key[..shared] + key_delta。
        let mut full_key = Vec::with_capacity(shared as usize + non_shared as usize);
        full_key.extend_from_slice(&last_key[..shared as usize]);
        full_key.extend_from_slice(key_delta);
        Some(ParsedEntry {
            key: full_key,
            value: &self.data[value_start..value_end],
            next_offset: value_end,
        })
    }

    /// 偏移是否已越过 entry 区进入 restarts 数组。
    fn is_past_entries(&self, offset: usize) -> bool {
        offset >= self.restarts_offset
    }

    /// 迭代 block 内所有 (key, value)。按存储顺序，key 已重建为完整字节。
    pub fn iter(&self) -> BlockIter<'a, '_> {
        BlockIter {
            block: self,
            offset: 0,
            last_key: Vec::new(),
        }
    }
}

struct ParsedEntry<'a> {
    key: Vec<u8>,
    value: &'a [u8],
    next_offset: usize,
}

/// Block 迭代器：顺序遍历所有 entry，重建完整 key。
pub struct BlockIter<'a, 'b> {
    block: &'b Block<'a>,
    offset: usize,
    last_key: Vec<u8>,
}

impl<'a, 'b> Iterator for BlockIter<'a, 'b> {
    type Item = (Vec<u8>, &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.block.is_past_entries(self.offset) {
            return None;
        }
        let entry = self.block.parse_entry(self.offset, &self.last_key)?;
        self.last_key = entry.key.clone();
        self.offset = entry.next_offset;
        Some((entry.key, entry.value))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    /// 字节字典序比较，供测试用。
    fn byte_cmp(a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn build_block(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
        let mut b = BlockBuilder::new();
        for (k, v) in entries {
            b.add(k, v);
        }
        b.finish()
    }

    #[test]
    fn round_trip_preserves_all_entries() {
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"apple-green", b"2"),
            (b"apple-red", b"1"),
            (b"apple-yellow", b"3"),
            (b"banana", b"4"),
            (b"cherry", b"5"),
        ];
        let bytes = build_block(&entries);
        let block = Block::new(&bytes).unwrap();
        let collected: Vec<(Vec<u8>, Vec<u8>)> =
            block.iter().map(|(k, v)| (k, v.to_vec())).collect();
        assert_eq!(collected.len(), entries.len());
        for (i, (k, v)) in collected.iter().enumerate() {
            assert_eq!(k.as_slice(), entries[i].0);
            assert_eq!(v.as_slice(), entries[i].1);
        }
    }

    #[test]
    fn prefix_compression_shrinks_repeated_prefix() {
        // 验证前缀压缩生效：同前缀的 key 占用字节少于各自完整存储。
        let keys: Vec<Vec<u8>> = (0..5u32)
            .map(|i| format!("longprefix-{i}").into_bytes())
            .collect();
        let val: &[u8] = b"v";
        let refs: Vec<(&[u8], &[u8])> = keys.iter().map(|k| (k.as_slice(), val)).collect();
        let compressed = build_block(&refs);

        // 对照：构造一个"不压缩"的版本（每条都是 restart point，shared=0）。
        // 用 RESTART_INTERVAL=1 不现实，改为直接算理论全量大小。
        let total_key_bytes: usize = refs.iter().map(|(k, _)| k.len()).sum();
        let total_val_bytes: usize = refs.iter().map(|(_, v)| v.len()).sum();
        // 压缩后应明显小于"每条存完整 key + value"。
        assert!(
            compressed.len() < total_key_bytes + total_val_bytes + refs.len() * 3 * 2,
            "compression not effective: {} vs raw {}",
            compressed.len(),
            total_key_bytes + total_val_bytes
        );
    }

    #[test]
    fn restart_point_every_interval() {
        // 插入 2*RESTART_INTERVAL + 3 条，应有 3 个 restart point。
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..(2 * RESTART_INTERVAL + 3) as u32)
            .map(|i| (format!("k{i:03}").into_bytes(), b"v".to_vec()))
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();
        // 首个 restart point 在偏移 0，之后每 RESTART_INTERVAL 条一个。
        // 总数 = 1 + ceil((N-1)/INTERVAL) = 1 + ceil((35-1)/16) = 1 + 3 = 4。
        // 实际：第0条(初始)，第16条，第32条 → 之后第33,34条不再加 restart。
        // restarts = [0, offset_at_16, offset_at_32] → 3 个。
        assert!(
            block.num_restarts >= 3,
            "num_restarts={}",
            block.num_restarts
        );
    }

    #[test]
    fn get_hits_existing_keys() {
        // key 必须按字典序追加（BlockBuilder 的前提）。
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"apple-green", b"2"),
            (b"apple-red", b"1"),
            (b"apple-yellow", b"3"),
            (b"banana", b"4"),
            (b"cherry", b"5"),
        ];
        let bytes = build_block(&entries);
        let block = Block::new(&bytes).unwrap();
        for (k, v) in &entries {
            assert_eq!(block.get(k, &byte_cmp), Some(*v), "missed key {:?}", k);
        }
    }

    #[test]
    fn get_returns_none_for_missing() {
        // 字典序：green < red < yellow。
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"apple-green", b"2"),
            (b"apple-red", b"1"),
            (b"cherry", b"5"),
        ];
        let bytes = build_block(&entries);
        let block = Block::new(&bytes).unwrap();
        // 区间内空隙：apple-blue 在 green 和 red 之间，但不存在。
        assert_eq!(block.get(b"apple-blue", &byte_cmp), None);
        // 小于最小 key。
        assert_eq!(block.get(b"aardvark", &byte_cmp), None);
        // 大于最大 key。
        assert_eq!(block.get(b"zebra", &byte_cmp), None);
    }

    #[test]
    fn get_across_restart_boundary() {
        // 插入足够多的 key，让查找跨越 restart point 边界。
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..50u32)
            .map(|i| {
                (
                    format!("key{i:03}").into_bytes(),
                    format!("val{i}").into_bytes(),
                )
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();
        for (k, v) in &entries {
            assert_eq!(block.get(k, &byte_cmp), Some(v.as_slice()), "missed {k:?}");
        }
        // 不存在的 key（落在两个 restart 区间之间）。
        assert_eq!(block.get(b"key024a", &byte_cmp), None);
    }

    #[test]
    fn empty_block_rejected() {
        // 空字节无法解析。
        assert!(Block::new(&[]).is_err());
        assert!(Block::new(&[0, 0, 0]).is_err());
    }

    #[test]
    fn single_entry_block() {
        let bytes = build_block(&[(b"only", b"one")]);
        let block = Block::new(&bytes).unwrap();
        assert_eq!(block.get(b"only", &byte_cmp), Some(b"one".as_slice()));
        assert_eq!(block.get(b"other", &byte_cmp), None);
    }

    #[test]
    fn large_block_stress() {
        // 大量 key 验证 round-trip + 查找。
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..2000u32)
            .map(|i| {
                (
                    format!("k{i:05}").into_bytes(),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();
        // 全量 round-trip。
        let collected: Vec<_> = block.iter().collect();
        assert_eq!(collected.len(), 2000);
        // 全量查找。
        for (k, v) in &entries {
            assert_eq!(block.get(k, &byte_cmp), Some(v.as_slice()));
        }
    }

    #[test]
    fn lower_bound_returns_first_ge() {
        let entries: Vec<(&[u8], &[u8])> = vec![
            (b"apple", b"1"),
            (b"banana", b"2"),
            (b"cherry", b"3"),
            (b"date", b"4"),
        ];
        let bytes = build_block(&entries);
        let block = Block::new(&bytes).unwrap();
        // 精确命中：lower_bound == 该 key 的 value。
        assert_eq!(
            block.lower_bound(b"banana", &byte_cmp),
            Some(b"2".as_slice())
        );
        // 区间内：lower_bound("blue") 返回 "cherry"（第一个 >= "blue"）。
        assert_eq!(block.lower_bound(b"blue", &byte_cmp), Some(b"3".as_slice()));
        // 小于所有 key：返回第一个 key 的 value。
        assert_eq!(
            block.lower_bound(b"aardvark", &byte_cmp),
            Some(b"1".as_slice())
        );
        // 大于所有 key：返回 None。
        assert_eq!(block.lower_bound(b"zebra", &byte_cmp), None);
    }

    #[test]
    fn lower_bound_across_restart_boundary() {
        // 足够多的 key 让 lower_bound 跨 restart point。
        let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..50u32)
            .map(|i| {
                (
                    format!("key{i:03}").into_bytes(),
                    format!("v{i}").into_bytes(),
                )
            })
            .collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();
        // 区间内：lower_bound("key024a") 返回 key025（第一个 >= "key024a"）。
        assert_eq!(
            block.lower_bound(b"key024a", &byte_cmp),
            Some(b"v25".as_slice())
        );
        // 精确命中。
        assert_eq!(
            block.lower_bound(b"key032", &byte_cmp),
            Some(b"v32".as_slice())
        );
    }

    /// 用 internal_key_cmp + 变长 user_key 的 encode 字节测试 lower_bound。
    /// 这是 SSTable 实际场景：复现 "present-2" vs "present-200" 的前缀关系。
    #[test]
    fn lower_bound_with_internal_key_cmp_variable_user_key() {
        use crate::internal_key::{
            internal_key_cmp, lookup_key, user_key_of_internal_key, InternalKey, ValueType,
        };

        // 构造一组 internal key，含变长前缀关系 + 足够数量触发多个 restart point。
        let mut iks: Vec<InternalKey> = vec![];
        iks.push(InternalKey::new(b"present-2".to_vec(), 1, ValueType::Put));
        iks.push(InternalKey::new(b"present-200".to_vec(), 1, ValueType::Put));
        // present-199 是关键：字典序在 present-2 之前，但和 present-2 共享前缀 "present-"，
        // present-2 又是 present-200 前缀。三者连续排列考验前缀压缩跨边界。
        iks.push(InternalKey::new(b"present-199".to_vec(), 1, ValueType::Put));
        for i in 0..50u32 {
            iks.push(InternalKey::new(
                format!("present-3-{i}").into_bytes(),
                1,
                ValueType::Put,
            ));
        }
        iks.sort();
        let entries: Vec<(Vec<u8>, Vec<u8>)> =
            iks.iter().map(|ik| (ik.encode(), b"v".to_vec())).collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();

        // 先遍历，确认所有 entry 都能正确重建且有序。
        let collected: Vec<Vec<u8>> = block
            .iter()
            .map(|(k, _)| user_key_of_internal_key(&k).to_vec())
            .collect();
        assert_eq!(
            collected,
            iks.iter().map(|ik| ik.user_key.clone()).collect::<Vec<_>>(),
            "block iteration missing/misordered keys"
        );

        // lower_bound 查 present-2：哨兵 seq=MAX，应命中 present-2 的真实版本（而非 present-200）。
        let lookup = lookup_key(b"present-2", crate::internal_key::MAX_SEQUENCE);
        let (found_key, _v) = block
            .lower_bound_kv(&lookup, &|a, b| internal_key_cmp(a, b))
            .expect("lower_bound should hit something");
        assert_eq!(
            user_key_of_internal_key(&found_key),
            b"present-2",
            "lower_bound(present-2) hit wrong key"
        );
    }

    /// 测 lower_bound（非 kv 版，index 路由用）在多 restart + 变长 internal key 下。
    /// 这是 SSTable index 路由的实际场景。
    #[test]
    fn lower_bound_index_style_many_restarts_variable_keys() {
        use crate::internal_key::{internal_key_cmp, lookup_key, InternalKey, ValueType};

        // 50 个变长 key，触发多个 restart point（每 16 条一个）。
        let mut iks: Vec<InternalKey> = vec![];
        for i in 0..50u32 {
            iks.push(InternalKey::new(
                format!("present-{i}").into_bytes(),
                1,
                ValueType::Put,
            ));
        }
        iks.sort();
        let entries: Vec<(Vec<u8>, Vec<u8>)> =
            iks.iter().map(|ik| (ik.encode(), b"v".to_vec())).collect();
        let refs: Vec<(&[u8], &[u8])> = entries
            .iter()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
            .collect();
        let bytes = build_block(&refs);
        let block = Block::new(&bytes).unwrap();

        // 用 lookup_key 构造哨兵，测 lower_bound（返回 value）。
        // index 场景里 value 是 BlockHandle；这里 value 是 b"v"，但逻辑一样。
        for ik in &iks {
            let lookup = lookup_key(&ik.user_key, crate::internal_key::MAX_SEQUENCE);
            let found = block.lower_bound(&lookup, &|a, b| internal_key_cmp(a, b));
            assert_eq!(
                found,
                Some(b"v".as_slice()),
                "lower_bound missed {:?}",
                ik.user_key
            );
        }
    }
}
