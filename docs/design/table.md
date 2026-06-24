# SSTable — 组装与读取

**文件布局**

```
[data block 0][data block 1]...[data block N-1]
[filter block]      ← 布隆裸字节
[metaindex block]   ← Block 格式，存 (key="filter.mulan.BloomFilter", value=filter_handle)
[index block]       ← Block 格式，每 data block 一条 (最大 key, handle)
[footer]            ← 48 字节，metaindex_handle + index_handle + padding + magic
```

**pending_index 机制的由来**

Index 项的 key 必须是"本 data block 的最大 key"，但只有等 data block **填满切出**时才知道最后一条是最大 key。然而 flush_data_block 时，还不知道后面还有没有更多 add 调用。所以暂存 `pending_index_key` 和 `pending_handle`，等到下一轮 `add` 再写入 index_block。如果没有下一轮 add（finish 时），由 finish 补写。

**footer 设计**

固定 48 字节，位于文件末尾。打开 SSTable 只需 `seek(end-48)` 一次读。永远存两个 handle：

```
footer: [metaindex_handle(varint)][index_handle(varint)][padding][MAGIC(8)]
```

**metaindex 的间接层**

metaindex block 是真正的 Block 格式，存 `"filter.mulan.BloomFilter" → filter_handle`。布隆裸字节是单独写的 filter block。加任何一种元数据（压缩字典、统计信息）只需在 metaindex block 里加一条 entry，footer 结构不变。

**`get_entry` 的 5 步查找路径**

1. 布隆过滤（[Bloom Filter](bloom.md)）：`bloom.may_contain(user_key) == false` → 直接返回 None
2. 构造哨兵 internal key：`lookup_key(user_key)`
3. [index block](block.md) `lower_bound`：定位到可能包含该 key 的 data block
4. data block 内 `lower_bound_kv`（[Block 前缀压缩](block.md)）：找到第一个 ≥ 哨兵的条目
5. user_key 校验：对比条目的 user_key 是否等于 target

**TableIter 惰性化**：`TableIter` 自持 `data: Vec<u8>`（`'static`），一次只解析一个 data block（~4KB），峰值内存远低于全量 collect 方案。实现了 `LsmIterator` trait，可直接 `Box::new(reader.into_table_iter()?)` 喂给 [MergingIterator](iterator.md)，避免 compaction 时全量加载。
