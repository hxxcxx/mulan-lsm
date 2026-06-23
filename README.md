# mulan-lsm

## 已完成的优化

### TableIter 惰性化：消除 compaction 的全量 collect

**问题**：`do_compaction` 原先用 `reader.iter().collect::<Vec<_>>()` 把每个输入 SSTable 的全部 entry 一次性收集成 Vec，再包成 `VecIterator` 喂给 `MergingIterator`。原因是 `TableIter<'a>` 借用 `&TableReader`，无法 move 成 `Box<dyn LsmIterator + 'static>`。

**根因**：`.collect()` 把 `TableIter` 本身的 block 级惰性（`advance_to_next_block` 一次一 block）彻底废掉——驱动迭代器到耗尽，全表 entry 进 Vec。

**改动**：
- `TableIter` 去掉 `'a`，自持 `data: Vec<u8>`，变 `'static`
- `TableIter` 实现 `LsmIterator`（与 `Iterator` 共存）
- `TableReader::into_table_iter(self)` 消费 self，move data 给 TableIter
- `compaction` 改用 `reader.into_table_iter()?`，直接 `Box` 进 `Vec<Box<dyn LsmIterator>>`，不再 collect
- `advance_to_next_block` 逻辑不变（一次一 block 的惰性本就对，只是之前被 collect 废了）

**收益**：compaction 读 N 个文件时，peak 内存从「N 个整表」降到「N 个 block（~4KB）」，约 500 倍；启动延迟从「等全量解完」降到零。

**验证**：`table_iter_is_lazy` 测试用 `blocks_loaded()` 断言——构造时 0、取 3 条只加载 1 block、提前 drop 远小于总 block 数；外加完整遍历吐全量（惰性不丢数据）。

**命名细节**：方法名用 `into_table_iter` 而非 `into_iter`（clippy 提示后者易与 `IntoIterator` trait 混淆）。
