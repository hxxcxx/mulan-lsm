//! MemTable：LSM 的可写内存层。
//!
//! 内部是一个跳表，key 存 InternalKey（user_key + seq + vtype），
//! value 存用户 value 字节。put 和 delete 都是"追加一个版本"，不原地修改。

use crate::error::Result;
use crate::internal_key::{InternalKey, ValueType, MAX_SEQUENCE};
use crate::skiplist::SkipList;
use crate::sstable::TableBuilder;

/// MemTable flush 的结果：写入条目数 + 首/尾 internal key（供 DB 构造 `FileMetaData`）。
/// 空 MemTable flush 时 smallest/largest 为 None。
pub struct FlushResult {
    pub num_entries: u64,
    pub smallest: Option<InternalKey>,
    pub largest: Option<InternalKey>,
}

/// LSM 的内存表。所有写入先落到这里，攒满后刷成 SSTable。
pub struct MemTable {
    skiplist: SkipList<InternalKey, Vec<u8>>,
    seq: u64,
    entries: usize,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            skiplist: SkipList::new(),
            seq: 0,
            entries: 0,
        }
    }

    /// 用给定的起始 seq 构造空 MemTable。供 flush 后新 MemTable 继承全局 seq 单调性使用。
    pub fn with_initial_sequence(seq: u64) -> Self {
        MemTable {
            skiplist: SkipList::new(),
            seq,
            entries: 0,
        }
    }

    /// 当前已分配的最大 sequence number。
    pub fn sequence(&self) -> u64 {
        self.seq
    }

    /// 当前 memtable 中的条目数（含删除标记）。
    pub fn num_entries(&self) -> usize {
        self.entries
    }

    /// 把 memtable 中 seq ≤ snapshot_seq 的全部 entry 收集成 Vec（按 Ord 有序）。
    /// 供 DBIter 扫描用——memtable 借用生命周期无法跨锁存活，故全量克隆。
    /// memtable 通常小（几 MB），克隆开销可接受。
    pub fn snapshot_entries(&self, snapshot_seq: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.skiplist
            .iter()
            .filter(|(ik, _)| ik.seq <= snapshot_seq)
            .map(|(ik, v)| (ik.encode(), v.clone()))
            .collect()
    }

    /// 写入一个键值对。每次写都分配一个新的 seq，作为独立版本插入跳表。
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.seq += 1;
        let ik = InternalKey::new(key.to_vec(), self.seq, ValueType::Put);
        self.skiplist.insert(ik, value.to_vec());
        self.entries += 1;
    }

    /// 删除一个键：写入一个删除标记（type=Delete），而非物理移除。
    /// 读取时遇到删除标记即视为该 key 不存在。
    /// 删除不存在的 key 也是合法的——只是追加一个删除标记。
    pub fn delete(&mut self, key: &[u8]) {
        self.seq += 1;
        let ik = InternalKey::new(key.to_vec(), self.seq, ValueType::Delete);
        // 删除标记的 value 为空。
        self.skiplist.insert(ik, Vec::new());
        self.entries += 1;
    }

    /// 用指定的 seq 应用一条写操作（put 或 delete），不重新分配 seq。
    /// 供 WAL 崩溃恢复回放使用——必须复用 record 里的原始 seq，
    /// 否则跨崩溃后 seq 不连续，会破坏未来的 snapshot/MVCC 引用。
    /// 同时把 seq 推进到 max(当前, record_seq)，保证后续写入继续递增。
    pub fn apply(&mut self, vtype: ValueType, seq: u64, key: &[u8], value: &[u8]) {
        let ik = InternalKey::new(key.to_vec(), seq, vtype);
        self.skiplist.insert(ik, value.to_vec());
        if seq > self.seq {
            self.seq = seq;
        }
        self.entries += 1;
    }

    /// 把 MemTable 的全部内容刷成一个 SSTable 文件，返回条目数。
    pub fn flush_to_sstable(&self, path: &std::path::Path) -> Result<u64> {
        self.flush_to_sstable_with_bounds(path)
            .map(|r| r.num_entries)
    }

    /// flush 并额外返回首/尾 internal key，供 DB 构造 `FileMetaData` 的 smallest/largest。
    /// 跳表按 `InternalKey` Ord 有序，首条即 smallest、末条即 largest。
    /// 使用默认的 block_target（4KB）和 bits_per_key（10）。
    pub fn flush_to_sstable_with_bounds(&self, path: &std::path::Path) -> Result<FlushResult> {
        self.flush_to_sstable_with_options(path, 4 * 1024, 10)
    }

    /// 同 `flush_to_sstable_with_bounds`，但可指定 data block 目标大小和布隆精度。
    pub fn flush_to_sstable_with_options(
        &self,
        path: &std::path::Path,
        block_target: usize,
        bits_per_key: usize,
    ) -> Result<FlushResult> {
        let file = std::fs::File::create(path)?;
        let mut builder = TableBuilder::with_options(file, block_target, bits_per_key);
        let mut smallest: Option<InternalKey> = None;
        let mut largest: Option<InternalKey> = None;
        for (ik, value) in self.skiplist.iter() {
            builder.add(&ik.user_key, &ik.encode(), value)?;
            if smallest.is_none() {
                smallest = Some(ik.clone());
            }
            largest = Some(ik.clone());
        }
        let num_entries = builder.num_entries();
        builder.finish()?;
        Ok(FlushResult {
            num_entries,
            smallest,
            largest,
        })
    }

    /// 查 key 的最新版本，返回 (vtype, value) 以便调用方区分"删除标记"与"未找到"。
    ///
    /// 多层 LSM 的读路径必须区分这两种情况：删除标记要屏蔽下层 SSTable 的旧版本，
    /// 而"未找到"才需要继续查下层。返回 `None` 表示 memtable 无此 user_key；
    /// `Some((Delete, _))` 表示找到删除标记；`Some((Put, v))` 表示找到有效值。
    /// 读取 key 在 snapshot_seq 时间点的版本。
    /// 哨兵用 (key, snapshot_seq)：lower_bound 命中同 user_key 下 seq ≤ snapshot 的最新版本。
    /// 传 MAX_SEQUENCE = 读最新版本。
    pub fn get_entry(&self, key: &[u8], snapshot_seq: u64) -> Result<Option<(ValueType, Vec<u8>)>> {
        let lookup = InternalKey::new(key.to_vec(), snapshot_seq, ValueType::Put);
        let Some((ik, value)) = self.skiplist.lower_bound(&lookup) else {
            return Ok(None);
        };
        if ik.user_key != key {
            return Ok(None);
        }
        Ok(Some((ik.vtype, value.clone())))
    }

    /// 读取 key 的最新版本。删除标记和未找到都返回 `None`（单层语义）。
    /// 多层读路径应改用 [`get_entry`](Self::get_entry) 以区分两者。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.get_entry(key, MAX_SEQUENCE)? {
            Some((ValueType::Put, v)) => Ok(Some(v)),
            _ => Ok(None),
        }
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    #[test]
    fn put_then_get() {
        let mut m = MemTable::new();
        m.put(b"k1", b"v1");
        assert_eq!(m.get(b"k1").unwrap(), Some(b"v1".to_vec()));
    }

    #[test]
    fn get_missing_key_returns_none() {
        let mut m = MemTable::new();
        m.put(b"a", b"1");
        assert_eq!(m.get(b"missing").unwrap(), None);
        // 空跳表也返回 None。
        assert_eq!(MemTable::new().get(b"any").unwrap(), None);
    }

    #[test]
    fn delete_returns_none() {
        let mut m = MemTable::new();
        m.put(b"k", b"v");
        assert_eq!(m.get(b"k").unwrap(), Some(b"v".to_vec()));
        m.delete(b"k");
        assert_eq!(m.get(b"k").unwrap(), None);
    }

    #[test]
    fn delete_missing_key_is_idempotent() {
        let mut m = MemTable::new();
        m.delete(b"never_existed");
        assert_eq!(m.get(b"never_existed").unwrap(), None);
    }

    #[test]
    fn latest_version_wins() {
        let mut m = MemTable::new();
        m.put(b"k", b"v1");
        m.put(b"k", b"v2");
        m.put(b"k", b"v3");
        assert_eq!(m.get(b"k").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn put_after_delete_revives() {
        let mut m = MemTable::new();
        m.put(b"k", b"v1");
        m.delete(b"k");
        m.put(b"k", b"v2");
        assert_eq!(m.get(b"k").unwrap(), Some(b"v2".to_vec()));
    }

    #[test]
    fn independent_keys() {
        let mut m = MemTable::new();
        m.put(b"apple", b"1");
        m.put(b"banana", b"2");
        m.put(b"cherry", b"3");
        m.delete(b"banana");
        assert_eq!(m.get(b"apple").unwrap(), Some(b"1".to_vec()));
        assert_eq!(m.get(b"banana").unwrap(), None);
        assert_eq!(m.get(b"cherry").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn sequence_monotonic() {
        let mut m = MemTable::new();
        assert_eq!(m.sequence(), 0);
        m.put(b"a", b"1");
        assert_eq!(m.sequence(), 1);
        m.delete(b"b");
        assert_eq!(m.sequence(), 2);
        m.put(b"c", b"3");
        assert_eq!(m.sequence(), 3);
    }

    /// 跨边界回归测试：曾经 InternalKey 用按位取反方案做排序键时，
    /// 在 seq=255 处崩溃（小端字节序与整数序不一致）。这里连续 put 到 seq 远超 255，
    /// 证明当前的 Ord 比较器方案在跨字节边界下正确。
    #[test]
    fn get_works_across_seq_byte_boundary() {
        let mut m = MemTable::new();
        // put 300 个不同 key，seq 从 1 到 300，跨越 255 边界。
        for i in 0..300u32 {
            m.put(&i.to_be_bytes(), format!("v{i}").as_bytes());
        }
        for i in 0..300u32 {
            let expected = Some(format!("v{i}").into_bytes());
            assert_eq!(
                m.get(&i.to_be_bytes()).unwrap(),
                expected,
                "failed at i={i}"
            );
        }
    }

    /// 差分测试：同样操作序列同时跑 MemTable 和 HashMap（参照实现），
    /// 逐条比对 get 结果。覆盖反复写、删后复活、跨 seq 边界等。
    #[test]
    fn differential_against_hashmap() {
        const OPS: usize = 5_000;
        const KEY_SPACE: usize = 200;
        let mut m = MemTable::new();
        let mut model: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        // 固定种子，保证失败可复现。
        let mut rng = StdRng::seed_from_u64(42);
        for i in 0..OPS {
            let key = (rng.random_range(0..KEY_SPACE) as u32)
                .to_be_bytes()
                .to_vec();
            if rng.random_range(0..3) == 0 {
                m.delete(&key);
                model.remove(&key);
            } else {
                let val = format!("v{}", rng.random_range(0..100_000)).into_bytes();
                m.put(&key, &val);
                model.insert(key.clone(), val);
            }
            let got = m.get(&key).unwrap();
            let expected = model.get(&key).cloned();
            assert_eq!(got, expected, "mismatch at op {i} on key {key:?}");
        }
        // 全量校验 model 中每个 key 的状态。
        for key in model.keys() {
            assert_eq!(m.get(key).unwrap(), model.get(key).cloned());
        }
    }

    #[test]
    #[ignore = "10万级差分压测，cargo test -- --ignored 运行"]
    fn stress_differential_hundred_thousand() {
        const OPS: usize = 100_000;
        const KEY_SPACE: usize = 5_000;
        let mut m = MemTable::new();
        let mut model: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        let mut rng = StdRng::seed_from_u64(7);
        for i in 0..OPS {
            let key = (rng.random_range(0..KEY_SPACE) as u32)
                .to_be_bytes()
                .to_vec();
            if rng.random_range(0..4) == 0 {
                m.delete(&key);
                model.remove(&key);
            } else {
                let val = format!("v{i}").into_bytes();
                m.put(&key, &val);
                model.insert(key.clone(), val);
            }
            if i % 1000 == 0 {
                let probe = (rng.random_range(0..KEY_SPACE) as u32)
                    .to_be_bytes()
                    .to_vec();
                assert_eq!(m.get(&probe).unwrap(), model.get(&probe).cloned());
            }
        }
    }
}
