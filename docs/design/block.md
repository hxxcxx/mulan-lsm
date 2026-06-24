# Block — 前缀压缩 + 二分锚点

**字节布局**

```
[entry 0][entry 1]...[entry N-1]
[restarts[0]:u32][restarts[1]:u32]...[restarts[M-1]:u32]
[num_restarts:u32]
```

每条 entry：`shared(varint) | non_shared(varint) | value_len(varint) | key_delta | value`

- `shared` = 与上一条 key 共享的前缀长度
- `non_shared` = 本条独有的 key 后缀长度
- 每 `RESTART_INTERVAL`（16）条设一个 restart point（`shared=0`，存完整 key）

**为什么有 restart point？** 前缀压缩后，完整 key 不在字节里连续存放，无法直接二分。restart point 作为二分锚点：先二分定位到重启区间（O(log n)），再线性扫描最多 16 条重建 key 比较（O(1)）。

**`entry_at` vs `parse_entry`**

- `entry_at`：解析 entry 的原始字节，返回 `key_delta`（后缀），**不重建完整 key**。只在 restart point（shared=0，key_delta 即完整 key）处调用
- `parse_entry`：接收 `last_key` 参数，拼接 `last_key[..shared] + key_delta` 重建完整 key。用在线性扫描和迭代器中

**`lower_bound` vs `get`**

- `get`：精确匹配，二分找区间 → 线性扫描到 **相等** 时返回
- `lower_bound`：找第一个 ≥ target 的，二分找区间 → 线性扫描到 **第一个 ≥** 时返回。用于 index block 路由（分隔符查找）

**`lower_bound_kv` 的额外返回值**：比 `lower_bound` 多返回重建后的完整 key（owned），因为 prefix compression 下完整 key 不在字节中连续存放。TableReader 需要拿 key 出来跟 target 比对 user_key 是否一致。

**历史教训**：曾引入 sort_key（`user_key + !seq 大端 + type`）试图让字节字典序反映 InternalKey Ord，在变长 user_key 的前缀关系下失败（如 "present-2" vs "present-200"）。最终放弃，改为 Block 接收比较器闭包，回归 LevelDB 原版思路。详见 [InternalKey Ord 设计](internal-key.md)。
