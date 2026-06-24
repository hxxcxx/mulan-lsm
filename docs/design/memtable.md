# MemTable — 关键是区分 Delete 与 NotFound

**`get_entry` 必须返回 `ValueType`**

```rust
pub fn get_entry(&self, key: &[u8]) -> Result<Option<(ValueType, Vec<u8>)>>
```

- `None`：跳表里没有这个 user_key → 需要继续查下层 SSTable
- `Some((Delete, _))`：有删除标记 → 屏蔽下层，直接返回 None
- `Some((Put, v))`：有有效值 → 返回

这是多层 LSM 读路径正确性的关键：DB 的 `get` 先查 MemTable，再查 Immutable MemTable，再逐层查 SSTable。如果不区分 Delete 和 NotFound，删除标记就无法屏蔽下层的旧版本。

**哨兵查找流程**：[InternalKey](internal-key.md) 的 seq 降序 + 哨兵约定让一次 `lower_bound` 就能命中最新版本。[SkipList](skiplist.md) 的 `find_prev` 以严格 `<` 遍历，保证重复 key 下大 seq 优先定位。
