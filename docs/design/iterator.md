# Iterator — 归并迭代器

**LsmIterator trait**：有序输出 `(internal_key_bytes, value_bytes)`，实现者保证按 `internal_key_cmp` 升序。

**MergingIterator（小顶堆归并）**：
- 每个源迭代器 peek 一条入堆
- 堆顶按 `internal_key_cmp` 排序（最小在顶）
- 输出堆顶后，推进该源重新入堆
- 不做同 user_key 去重——全部 internal key 有序输出，去重责任上移给调用方（compaction 按 oldest_snapshot_seq 判定、DBIter 取每 user_key 最新）

去重依赖 [InternalKey Ord](internal-key.md)：同 user_key 大 seq 在前（Ord 最小），堆顶即最新版本。

**VecIterator**：把 `Vec<(Vec<u8>, Vec<u8>)>` 包装成 LsmIterator，测试用。

**DBIter**：包装 MergingIterator，按 `snapshot_seq` 过滤（跳过 seq > snapshot 的版本），每 user_key 只取最新版本，跳过 Delete 标记，输出纯净的 `(user_key, value)` 供 range scan。实现为 Rust `Iterator` trait。
