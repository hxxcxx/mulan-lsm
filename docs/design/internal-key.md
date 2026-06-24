# InternalKey — 多版本的灵魂

**排序：Ord 与字节布局解耦**

```rust
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub vtype: ValueType,
}
```

排序规则（手动 `Ord`）：
1. `user_key` 字典序升序
2. 同 `user_key` 下 `seq` **降序**（大 seq = 新版本，排前面）
3. `seq` 相同时按 `vtype` 兜底（全序完备）

**为什么 seq 要降序？** 这是"命中即最新"的关键：
- MemTable 查找：哨兵 `(user_key, MAX_SEQUENCE)` 用 `lower_bound` 找"第一个 ≥ 哨兵"的条目，seq 降序下大 seq 在 Ord 上更小，所以命中的第一条真实条目就是最新版本
- SSTable 查找：Block 内同样按 Ord 存储，`lower_bound` 命中即最新
- MergingIterator 归并：小顶堆顶是 Ord 最小的条目 = 所有源中的最新版本

**encode() 的字节布局不参与排序**

```rust
pub fn encode(&self) -> Vec<u8> {
    // user_key + seq(小端) + type
    // 仅供 WAL/SSTable 持久化，排序由 Ord 决定
}
```

小端 seq 的字节序与整数序不一致（`seq=256` 的字节序小于 `seq=255`），所以 encode 字节直接当排序键会错乱。Block 存储 encode 字节，查找时用 `internal_key_cmp` 比较器（按 Ord 语义解析后比较），而非纯字节字典序。

**哨兵 `lookup_key`**

```rust
pub fn lookup_key(user_key: &[u8]) -> Vec<u8> {
    InternalKey::new(user_key.to_vec(), MAX_SEQUENCE, ValueType::Put).encode()
}
```

`MAX_SEQUENCE` 比所有真实 seq 大，在降序规则下 Ord 最小（排最前）。`lower_bound(哨兵)` 找到的第一个 ≥ 哨兵的条目，就是同 user_key 的真实最新版本。这个约定在 [MergingIterator](iterator.md) 中同样生效——堆顶即所有源中 Ord 最小的条目，即最新版本。

**历史教训**：曾试图用"按位取反 `!seq` + 小端"让字节字典序反映 Ord，在 seq=255 处崩溃。最终采用包装类型手动 Ord，**排序与字节布局彻底解耦**。
