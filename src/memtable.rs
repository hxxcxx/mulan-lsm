//! MemTable：LSM 的可写内存层。
//!
//! 内部是一个跳表，key 存 internal key 字节（user_key + !seq + type），
//! value 存用户 value 字节。put 和 delete 都是"追加一个版本"，不原地修改。

use crate::error::Result;
use crate::internal_key::{self, ValueType, MAX_SEQUENCE};
use crate::skiplist::SkipList;

/// LSM 的内存表。所有写入先落到这里，攒满后刷成 SSTable。
pub struct MemTable {
    skiplist: SkipList<Vec<u8>, Vec<u8>>,
    seq: u64,
}

impl MemTable {
    pub fn new() -> Self {
        MemTable {
            skiplist: SkipList::new(),
            seq: 0,
        }
    }

    /// 当前已分配的最大 sequence number。
    pub fn sequence(&self) -> u64 {
        self.seq
    }

    /// 写入一个键值对。每次写都分配一个新的 seq，作为独立版本插入跳表。
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.seq += 1;
        let internal_key = internal_key::make(key, self.seq, ValueType::Put);
        self.skiplist.insert(internal_key, value.to_vec());
    }

    /// 删除一个键：写入一个删除标记（type=Delete），而非物理移除。
    /// 读取时遇到删除标记即视为该 key 不存在。
    /// 删除不存在的 key 也是合法的——只是追加一个删除标记。
    pub fn delete(&mut self, key: &[u8]) {
        self.seq += 1;
        let internal_key = internal_key::make(key, self.seq, ValueType::Delete);
        // 删除标记的 value 为空。
        self.skiplist.insert(internal_key, Vec::new());
    }

    /// 读取 key 的最新版本。返回值克隆一份交给调用方拥有。
    ///
    /// 用哨兵技巧定位：构造 lookup_key = make(key, MAX_SEQUENCE, ...)，
    /// 由于"大 seq 在前"，它排在 key 所有真实版本的最前面。
    /// 取跳表中第一个 >= lookup_key 的条目，若其 user_key == key 即命中，
    /// 再按 type 决定是返回 value 还是 NotFound。
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let lookup_key = internal_key::make(key, MAX_SEQUENCE, ValueType::Put);
        let Some((internal_key_bytes, value)) = self.skiplist.lower_bound(&lookup_key) else {
            // 跳表里没有任何 >= lookup_key 的条目，key 从未写过。
            return Ok(None);
        };
        let parsed = internal_key::parse(internal_key_bytes)?;
        if parsed.user_key != key {
            // 第一个 >= lookup_key 的条目属于别的 user_key，本 key 未写过。
            return Ok(None);
        }
        match parsed.vtype {
            ValueType::Put => Ok(Some(value.clone())),
            ValueType::Delete => Ok(None),
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
        // 删除不存在的 key 不报错。
        m.delete(b"never_existed");
        assert_eq!(m.get(b"never_existed").unwrap(), None);
    }

    #[test]
    fn latest_version_wins() {
        let mut m = MemTable::new();
        m.put(b"k", b"v1");
        m.put(b"k", b"v2");
        m.put(b"k", b"v3");
        // get 返回最新版本 v3。
        assert_eq!(m.get(b"k").unwrap(), Some(b"v3".to_vec()));
    }

    #[test]
    fn put_after_delete_revives() {
        let mut m = MemTable::new();
        m.put(b"k", b"v1");
        m.delete(b"k");
        // 重新写入后又能读到。
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
}
