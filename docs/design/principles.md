# 通用原则

**RAII 与 `?` 简化错误处理**
- `MulanError` + `Result<T>` 统一错误类型，避开 `anyhow`
- 不混用 `unwrap`（测试代码除外）

**二分 boundary 的测试**：小值测试掩盖不了跨字节边界的 bug。test `internal_key_ord_correctness` 覆盖了 seq=255/256 跨边界、前缀关系（present-2 vs present-200）、MAX_SEQUENCE vs 0 等极端情况。

**Block 的 `lower_bound` vs `get`**：`get` 是精确查找，用于 metaindex 的 key 查找；`lower_bound` 是 ≥ 查找，用于 index 路由（找第一个分隔符 ≥ target）和 data block 内查找（找第一个 ≥ 哨兵的条目）。两者用途不同，不能互相替代。

**`std::mem::take` 复位语法**：`let bytes = std::mem::take(&mut self.data_block).finish()` 把当前 block 换出（用 Default 空壳替换），在空壳上继续 add，换出的旧 block 转成字节写入。免去手动 `BlockBuilder::new()` + 赋值。

**unwrap 的安全边界**：`current_builder.as_ref().unwrap()` 出现在 "is_none() → 建 builder → continue" 之后，靠控制流保证安全。但这是隐式契约——如果在 `is_none()` 和 `unwrap()` 之间意外清空 builder 就会崩。更安全的写法是用 `if let Some(builder) = &current_builder` 替代 `unwrap()`。
