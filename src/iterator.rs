//! 统一迭代器抽象 + 多路归并 + 用户层迭代器。
//!
//! - `LsmIterator`：输出 (internal_key_bytes, value_bytes) 的有序流，按 `internal_key_cmp` 升序。
//! - `MergingIterator`：多个 `LsmIterator` 多路归并，同 user_key 去重只留最新版本（最大 seq）。
//!   依赖 `InternalKey` Ord：同 user_key 大 seq 在前，故堆顶（Ord 最小）即最新版本。
//! - `DBIter`：包装 `MergingIterator`，把 internal key 转成 user key + 跳过删除标记，
//!   供用户层 range scan 用（M6）。

use crate::internal_key::{internal_key_cmp, user_key_of_internal_key, vtype_of_internal_key};
use crate::internal_key::{InternalKey, ValueType};
use std::cmp::Ordering;

/// LSM 内部有序迭代器：流式输出 (internal_key_bytes, value_bytes)。
/// 实现者保证输出按 `internal_key_cmp` 严格升序。
pub trait LsmIterator {
    /// 预览当前条目而不推进。`None` 表示已耗尽。
    fn peek(&self) -> Option<(&[u8], &[u8])>;

    /// 推进到下一条。返回被跳过的当前条目（与 peek 相同），或 `None` 若已耗尽。
    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)>;
}

/// 把一个 owned `(key, value)` 缓存成可被 trait 对象引用的条目。
/// `MergingIterator` 的堆元素持有此结构，peek 返回其内部借用。
struct Buffered {
    key: Vec<u8>,
    value: Vec<u8>,
    /// 来自哪个源迭代器（堆中区分同 key 来源）。
    src: usize,
}

impl Buffered {
    fn peek(&self) -> (&[u8], &[u8]) {
        (&self.key, &self.value)
    }
}

/// 多路归并迭代器。
///
/// 堆顶是 `internal_key_cmp` 最小的条目——由 Ord 定义，同 user_key 下大 seq 在前，
/// 故堆顶 = 当前 user_key 的最新版本。`next` 输出堆顶后，弹出所有同 user_key 的后续条目
/// （去重），再推进各源迭代器重新入堆。
pub struct MergingIterator {
    iters: Vec<Box<dyn LsmIterator>>,
    /// 堆：按 `internal_key_cmp` 排序，最小在顶。用 Vec + 手动 sift 模拟二叉堆。
    heap: Vec<Buffered>,
}

impl MergingIterator {
    pub fn new(iters: Vec<Box<dyn LsmIterator>>) -> Self {
        let mut merger = MergingIterator {
            iters,
            heap: Vec::new(),
        };
        // 初始化：每个源 peek 一次入堆。
        for src in 0..merger.iters.len() {
            if let Some((k, v)) = merger.iters[src].next() {
                merger.push(Buffered {
                    key: k,
                    value: v,
                    src,
                });
            }
        }
        merger
    }

    fn push(&mut self, item: Buffered) {
        self.heap.push(item);
        self.sift_up(self.heap.len() - 1);
    }

    fn pop(&mut self) -> Option<Buffered> {
        if self.heap.is_empty() {
            return None;
        }
        let last = self.heap.len() - 1;
        self.heap.swap(0, last);
        let item = self.heap.pop();
        if !self.heap.is_empty() {
            self.sift_down(0);
        }
        item
    }

    fn cmp_buffered(a: &Buffered, b: &Buffered) -> Ordering {
        internal_key_cmp(&a.key, &b.key)
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let parent = (i - 1) / 2;
            if Self::cmp_buffered(&self.heap[i], &self.heap[parent]) == Ordering::Less {
                self.heap.swap(i, parent);
                i = parent;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let mut smallest = i;
            let left = 2 * i + 1;
            let right = 2 * i + 2;
            if left < n
                && Self::cmp_buffered(&self.heap[left], &self.heap[smallest]) == Ordering::Less
            {
                smallest = left;
            }
            if right < n
                && Self::cmp_buffered(&self.heap[right], &self.heap[smallest]) == Ordering::Less
            {
                smallest = right;
            }
            if smallest == i {
                break;
            }
            self.heap.swap(i, smallest);
            i = smallest;
        }
    }

    fn peek_top(&self) -> Option<(&[u8], &[u8])> {
        self.heap.first().map(|b| b.peek())
    }
}

impl LsmIterator for MergingIterator {
    fn peek(&self) -> Option<(&[u8], &[u8])> {
        self.peek_top()
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let top = self.pop()?;
        let top_user_key = user_key_of_internal_key(&top.key).to_vec();
        let result = (top.key.clone(), top.value.clone());
        // 推进产出 top 的源迭代器，重新入堆。
        if let Some((k, v)) = self.iters[top.src].next() {
            self.push(Buffered {
                key: k,
                value: v,
                src: top.src,
            });
        }
        // 去重：弹出所有与 top 同 user_key 的条目（它们的 seq 更小 = 旧版本，丢弃）。
        while let Some((peek_key, _)) = self.peek_top() {
            if user_key_of_internal_key(peek_key) == top_user_key.as_slice() {
                let dup = self.pop().unwrap();
                if let Some((k, v)) = self.iters[dup.src].next() {
                    self.push(Buffered {
                        key: k,
                        value: v,
                        src: dup.src,
                    });
                }
            } else {
                break;
            }
        }
        Some(result)
    }
}

/// 把一个 `Vec<(Vec<u8>, Vec<u8>)>` 包装成 `LsmIterator`（测试 / 内存迭代用）。
pub struct VecIterator {
    items: Vec<(Vec<u8>, Vec<u8>)>,
    pos: usize,
}

impl VecIterator {
    pub fn new(items: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        VecIterator { items, pos: 0 }
    }
}

impl LsmIterator for VecIterator {
    fn peek(&self) -> Option<(&[u8], &[u8])> {
        self.items
            .get(self.pos)
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        let item = self.items.get(self.pos)?.clone();
        self.pos += 1;
        Some(item)
    }
}

/// 用户层迭代器：把 internal key 流转成 (user_key, value) 流，跳过删除标记。
///
/// 供 M6 range scan 用。内部委托 `MergingIterator` 做归并和去重，对每个最新版本：
/// - `Put` → 输出 (user_key, value)
/// - `Delete` → 跳过（用户视角该 key 不存在）
pub struct DBIter {
    inner: MergingIterator,
    /// 预读的下一条（user_key, value），供 peek。None 表示耗尽或下一条是 Delete 被跳过。
    pending: Option<(Vec<u8>, Vec<u8>)>,
}

impl DBIter {
    pub fn new(inner: MergingIterator) -> Self {
        let mut iter = DBIter {
            inner,
            pending: None,
        };
        iter.advance();
        iter
    }

    /// 推进内部迭代器，跳过 Delete，把下一个 Put 存进 pending。
    fn advance(&mut self) {
        self.pending = None;
        while let Some((ik_bytes, value)) = self.inner.next() {
            let vtype = vtype_of_internal_key(&ik_bytes);
            if vtype == ValueType::Put {
                let user_key = user_key_of_internal_key(&ik_bytes).to_vec();
                self.pending = Some((user_key, value));
                return;
            }
            // Delete：跳过（MergingIterator 已去重，这条是最新版本且是删除标记 → 该 key 不存在）。
        }
    }
}

impl DBIter {
    /// 预览当前 (user_key, value)。None 表示耗尽。
    pub fn peek(&self) -> Option<(&[u8], &[u8])> {
        self.pending
            .as_ref()
            .map(|(k, v)| (k.as_slice(), v.as_slice()))
    }
}

impl Iterator for DBIter {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.pending.take()?;
        self.advance();
        Some(item)
    }
}

/// 把 internal key 字节解析成 InternalKey（测试辅助）。
pub fn parse_internal_key(bytes: &[u8]) -> Option<InternalKey> {
    InternalKey::decode(bytes).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::internal_key::{InternalKey, ValueType};

    fn ik(user_key: &[u8], seq: u64, vtype: ValueType, value: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (
            InternalKey::new(user_key.to_vec(), seq, vtype).encode(),
            value.to_vec(),
        )
    }

    fn vec_iter(items: Vec<(Vec<u8>, Vec<u8>)>) -> Box<dyn LsmIterator> {
        Box::new(VecIterator::new(items))
    }

    #[test]
    fn merging_empty_iters_ends_immediately() {
        let mut m = MergingIterator::new(vec![]);
        assert!(m.next().is_none());
    }

    #[test]
    fn merging_single_iter_passthrough() {
        let items = vec![
            ik(b"a", 1, ValueType::Put, b"v1"),
            ik(b"b", 1, ValueType::Put, b"v2"),
        ];
        let mut m = MergingIterator::new(vec![vec_iter(items.clone())]);
        let out: Vec<_> = std::iter::from_fn(|| m.next()).collect();
        assert_eq!(out, items);
    }

    #[test]
    fn merging_two_iters_global_order() {
        // 两个有序 iter，key 区间交错，归并后全局有序。
        let a = vec![
            ik(b"a", 1, ValueType::Put, b"1"),
            ik(b"c", 1, ValueType::Put, b"3"),
        ];
        let b = vec![
            ik(b"b", 1, ValueType::Put, b"2"),
            ik(b"d", 1, ValueType::Put, b"4"),
        ];
        let mut m = MergingIterator::new(vec![vec_iter(a), vec_iter(b)]);
        let keys: Vec<Vec<u8>> = std::iter::from_fn(|| m.next()).map(|(k, _)| k).collect();
        let user_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| user_key_of_internal_key(k).to_vec())
            .collect();
        assert_eq!(
            user_keys,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[test]
    fn merging_dedups_same_user_key_keeps_latest_seq() {
        // 两个 iter 都有 user_key "k"，归并后只输出 seq 最大的（最新版本）。
        let a = vec![ik(b"k", 1, ValueType::Put, b"old")];
        let b = vec![ik(b"k", 5, ValueType::Put, b"new")];
        let mut m = MergingIterator::new(vec![vec_iter(a), vec_iter(b)]);
        let out: Vec<_> = std::iter::from_fn(|| m.next()).collect();
        assert_eq!(out.len(), 1, "duplicate user_key should be deduped");
        let parsed = InternalKey::decode(&out[0].0).unwrap();
        assert_eq!(parsed.seq, 5, "latest seq (5) should win");
        assert_eq!(out[0].1, b"new");
    }

    #[test]
    fn merging_dedups_across_three_iters() {
        // 三个 iter 各有 user_key "k" 的不同 seq，归并后只剩 seq 最大的。
        let a = vec![ik(b"k", 1, ValueType::Put, b"v1")];
        let b = vec![ik(b"k", 3, ValueType::Put, b"v3")];
        let c = vec![ik(b"k", 2, ValueType::Put, b"v2")];
        let mut m = MergingIterator::new(vec![vec_iter(a), vec_iter(b), vec_iter(c)]);
        let out: Vec<_> = std::iter::from_fn(|| m.next()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(InternalKey::decode(&out[0].0).unwrap().seq, 3);
    }

    #[test]
    fn merging_preserves_delete_as_latest() {
        // 最新版本是 Delete：归并输出它（保留删除标记，由 DBIter/compaction 决定是否跳过）。
        let a = vec![ik(b"k", 1, ValueType::Put, b"old")];
        let b = vec![ik(b"k", 2, ValueType::Delete, b"")];
        let mut m = MergingIterator::new(vec![vec_iter(a), vec_iter(b)]);
        let out: Vec<_> = std::iter::from_fn(|| m.next()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(
            InternalKey::decode(&out[0].0).unwrap().vtype,
            ValueType::Delete
        );
    }

    #[test]
    fn db_iter_skips_delete_entries() {
        let items = vec![
            ik(b"a", 1, ValueType::Put, b"va"),
            ik(b"b", 1, ValueType::Put, b"vb"),
            ik(b"c", 2, ValueType::Delete, b""),
            ik(b"d", 1, ValueType::Put, b"vd"),
        ];
        let m = MergingIterator::new(vec![vec_iter(items)]);
        let mut dbi = DBIter::new(m);
        let out: Vec<(Vec<u8>, Vec<u8>)> = std::iter::from_fn(|| dbi.next()).collect();
        // c 被删除标记跳过。
        let keys: Vec<Vec<u8>> = out.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"d".to_vec()]);
        assert_eq!(out[1].1, b"vb");
    }

    #[test]
    fn db_iter_delete_shadows_older_put() {
        // 同 user_key 最新是 Delete → DBIter 不输出该 key（旧 Put 被去重丢弃，Delete 被跳过）。
        // 注意 items 必须按 internal_key_cmp 升序：同 user_key 下大 seq 在前。
        let items = vec![
            ik(b"k", 2, ValueType::Delete, b""),
            ik(b"k", 1, ValueType::Put, b"old"),
            ik(b"x", 1, ValueType::Put, b"vx"),
        ];
        let m = MergingIterator::new(vec![vec_iter(items)]);
        let mut dbi = DBIter::new(m);
        let out: Vec<(Vec<u8>, Vec<u8>)> = std::iter::from_fn(|| dbi.next()).collect();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].0, b"x");
    }

    #[test]
    fn db_iter_empty_input() {
        let m = MergingIterator::new(vec![]);
        let mut dbi = DBIter::new(m);
        assert!(dbi.next().is_none());
    }

    #[test]
    fn merging_interleaved_user_keys_with_versions() {
        // 混合场景：不同 user_key 交错 + 同 user_key 多版本。
        let a = vec![
            ik(b"a", 1, ValueType::Put, b"a1"),
            ik(b"b", 1, ValueType::Put, b"b1"),
        ];
        let b = vec![
            ik(b"a", 5, ValueType::Put, b"a5"),
            ik(b"c", 1, ValueType::Put, b"c1"),
        ];
        let mut m = MergingIterator::new(vec![vec_iter(a), vec_iter(b)]);
        let out: Vec<_> = std::iter::from_fn(|| m.next()).collect();
        // a 只剩 seq=5，b 保留，c 保留。
        assert_eq!(out.len(), 3);
        let parsed: Vec<_> = out
            .iter()
            .map(|(k, _)| InternalKey::decode(k).unwrap())
            .collect();
        assert_eq!(parsed[0].user_key, b"a");
        assert_eq!(parsed[0].seq, 5);
        assert_eq!(parsed[1].user_key, b"b");
        assert_eq!(parsed[2].user_key, b"c");
    }
}
