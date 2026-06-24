# Version — 不可变快照

Version 是不可变的（创建后不可更改），通过 `apply_edit` 生成新 Version 并递增引用计数。`Arc<Version>` 共享读。

**NUM_LEVELS = 7**。各层语义：
- Level 0：文件区间可能重叠，读时要全查
- Level 1+：文件区间不重叠，二分定位到至多一个文件

**get_overlaps**：查某层中与 key 区间 [smallest, largest] 重叠的文件集合。利用 L1+ 层内文件有序（按 smallest 字典序排列），用 `partition_point` 二分定位到区间起点，线性延伸至首个不重叠的文件。返回结果用于：
- Compaction 选下一层输入文件：`inputs[1] = version.get_overlaps(level+1, range_smallest, range_largest)`
- 祖父层重叠计算：查 level+2 中所有与输入区间重叠的祖父文件
- 删除标记判断：`is_base_level_for_user_key` 检查 level+2 起每层是否还有该 key

**Snapshot 支持**：`acquire_snapshot(seq)` 将 seq 注册到快照列表，`release_snapshot(seq)` 移除。`oldest_snapshot_seq()` 返回最老快照的 seq（无快照时返回 `MAX_SEQUENCE`），compaction 据此决定哪些旧版本不能回收。
