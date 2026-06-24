# 待改进项

从全项目 63 个问题点中筛选，已修复 19 项，剩余按优先级排列。

---

## 一、性能类

### 1.1 Manifest Snapshot / Checkpoint (#29)

**现状**：`VersionSet::recover` 每次从头回放全部 `VersionEdit`。运行数月后 manifest 积累几十万条 edit，恢复时间线性增长。

**方案**：定期写全量快照到新 manifest 文件，恢复时从最近 snapshot 开始回放。LevelDB 有此机制。

**难度**：中（需新增 snapshot 格式 + 触发策略 + 旧 manifest 清理）

---

### 1.2 TableReader 全量读入内存 (#43)

**现状**：`TableReader::open` 将整个 SSTable 文件 `std::fs::read` 到 `Vec<u8>`。数 GB 的 SSTable 会 OOM。

**方案**：按需读 block（`pread` / `mmap`）。`Options` 增加 `max_sstable_mmap_size` 阈值，超过则用 `mmap`。

**难度**：中（需改动 TableReader + TableIter + 平台适配）

---

### 1.3 `new_iterator_at` 不使用 TableCache (#54)

**现状**：scan 路径每个 SSTable 都 `TableReader::open` 重解析，不走缓存。虽然 `into_table_iter` 需要 move data 导致无法直接复用 `Arc`，但 footer/index 解析结果可缓存。

**方案**：缓存解析好的 index block handle 列表，避免重复解析 footer/metaindex。

**难度**：中低

---

### 1.4 L0 Compaction 全选策略过于激进 (#51)

**现状**：`pick_compaction` 对 L0 全选所有文件。L0 达到 12 个时写阻塞，一次 compaction 输入可达 12 个文件，归并内存开销大。

**方案**：实现基于 compact_pointer 的增量选择（已预留 `TAG_COMPACT_POINTER`），只选与 pointer 之后区间重叠的 L0 文件。LevelDB 行为。

**难度**：低（算法已有，只差 L0 侧实现）

---

### 1.5 Manifest 每次 append 都 sync (#26)

**现状**：`ManifestWriter::append` 每条 edit 都调用 `wal.sync()`，flush + compaction 连续发生时 fsync 次数多。

**方案**：增加 `append_no_sync` + `sync` 批量提交接口，或仅在 compaction 提交时 sync。

**难度**：低（需保证"提交点"语义不变）

---

### 1.6 Skiplist `find_prev` 仍遍历已初始化为 HEAD 的高层

**现状**：`find_prev` 固定数组 `[usize; MAX_HEIGHT]` 遍历 `max_height` 层，但在新节点高度超过 `max_height` 时，超出的层前驱已默认填充为 HEAD。

**方案**：`random_height` 上限可随数据量动态调整，或保持 MAX_HEIGHT=12（LevelDB 用 12）。

**难度**：低

---

### 1.7 TableIter 全量解析 data block (#44)

**现状**：`TableIter::advance_to_next_block` 解析整个 data block 的全部 entry 到 `current: Vec<(Vec<u8>, Vec<u8>)>`，每条 value 都要 clone。对 compaction 路径，这些 value 仅短暂存活即被归并输出。

**方案**：`TableIter` 持有 `data: Vec<u8>`（`'static`），可直接返回 `&[u8]` 引用，避免每 entry value clone。

**难度**：中（需改动 LsmIterator trait 或 TableIter 内部）

---

## 二、功能类

### 2.1 写阻塞（Write Stall）粒度

**现状**：`level0_needs_slowdown` 只检查 L0 文件数 ≥ 12。LevelDB 还有"等待 compaction 完成"的慢写模式（L0 文件数 ≥ 8 时延迟写入）。

**方案**：实现分级写阻塞：
- L0 ≥ 8：每次写 sleep 1ms（软阻塞）
- L0 ≥ 12：硬阻塞等待 compaction（已实现）

**难度**：低

---

### 2.2 Seek Compaction 触发

**现状**：`FileMetaData.allowed_seeks` 字段已定义但未使用。LevelDB 在点查时递增此计数，超阈值时触发该文件所在区间的 compaction。

**方案**：在 `TableReader::get_entry` 成功后递增 `allowed_seeks`，后台检查是否需要 seek compaction。

**难度**：中（需跨层传递计数 + 选文件逻辑）

---

### 2.3 多后台 Compaction 线程 (#22)

**现状**：`IdGenerator` 明确标注"单线程使用"。当前只有一个后台 compaction 线程，安全但吞吐受限。

**方案**：将 `IdGenerator` 改为 `AtomicU64` 实现，支持多线程并发分配文件编号。后台线程池执行多个 compaction。

**难度**：中（需同步多 compaction 之间的冲突 + 编号原子化）

---

### 2.4 Snapshot 列表优化 (#31)

**现状**：`acquire_snapshot` / `release_snapshot` 用 `Vec<u64>` 的 O(n) 查找删除。

**方案**：快照数量通常很少（个位数），当前可接受。若快照变多可换 `BTreeSet`。

**难度**：低

---

## 三、健壮性类

### 3.1 WAL Fragment 无上限 (#19)

**现状**：`WalReader::read_records` 中 `fragment` 一直拼装 FIRST/MIDDLE 直到 LAST。恶意或损坏的 WAL 文件中 fragment 无限增长。

**方案**：增加 `fragment` 大小上限（如 64MB），超过则视为损坏停止。

**难度**：低

---

### 3.2 WAL 从非 block 边界恢复的残片 (#18)

**现状**：`WalWriter::create` 从 `文件大小 % BLOCK_SIZE` 开始写。若上次崩溃在 block 中间，恢复时这些残片靠 CRC 校验自动丢弃。行为正确，但文件中有不可读残片字节。

**方案**：恢复时 truncate 到最后一个完整 block（`len - len % BLOCK_SIZE`）。LevelDB 也有此行为。

**难度**：低

---

### 3.3 Flush 失败后孤儿 SSTable (#57)

**现状**：`flush_memtable` 中若 `flush_to_sstable_with_options` 成功但后续 `write_new_version` 失败，已写入的 SSTable 文件成为孤儿，直到下次 `open` 时 `remove_obsolete_files` 才清理。

**方案**：在 flush 失败路径上增加清理已写文件的逻辑，或标记为临时文件。

**难度**：低

---

### 3.4 Manifest Decode 缺少 Level 范围校验 (#25)

**现状**：`VersionEdit::decode` 中 `TAG_NEW_FILE` 不校验 level 范围，超界要到 `apply_edit` 才报错。

**方案**：解码时增加 `level < NUM_LEVELS` 检查，早期失败 = 更清晰的错误消息。

**难度**：低

---

### 3.5 VersionEdit 未知 Tag 硬错误 (#24)

**现状**：`decode` 遇到未知 tag 返回 `Corrupted`，不允许向前兼容。如果未来加新字段，旧版本打不开新 manifest。

**方案**：如果要支持滚动升级，应改为跳过未知 tag。当前严格模式可保留，需在文档中说明。

**难度**：低

---

## 四、代码质量类

### 4.1 Block 三个查找方法去重 (#36)

**现状**：`Block::get` / `lower_bound` / `lower_bound_kv` 三个方法共享"二分 restart → 线性扫描"结构，代码高度重复。

**方案**：提取通用二分查找 restart 区间的辅助方法，三个方法各自做线性扫描。

**难度**：低

---

### 4.2 Skiplist `get` / `lower_bound` 去重 (#14)

**现状**：两个方法下降逻辑完全相同，仅最后一步不同（`get` 判等，`lower_bound` 直接取后继）。

**方案**：提取 `fn seek_to_ge(&self, key: &K) -> (usize, bool)` 返回 (前驱索引, 是否精确命中)。

**难度**：低

---

### 4.3 Varint 编码泛型统一 (#3)

**现状**：`encode_varint32` 和 `encode_varint64` 代码几乎完全相同。

**方案**：泛型或宏统一。

**难度**：低

---

### 4.4 CRC32C 静态缓存 (#21)

**现状**：`crc32c` 函数每次调用都重新创建 `Crc<u32>` 对象（虽然 `const` 可减少开销）。

**方案**：存为 `static CRC32C: Crc<u32> = ...`。

**难度**：低

---

### 4.5 `VecIterator` 公开暴露 (#34)

**现状**：`VecIterator` 定义在 `iterator.rs` 并 `pub`，但生产代码中仅在 `new_iterator_at` 使用（包装 MemTable 快照条目）。对库使用者无价值。

**方案**：改为 `pub(crate)`，隐藏实现细节。

**难度**：低

---

### 4.6 跳表 `Node::new` 优化 (#11)

**现状**：`Node::new` 先 `Vec::with_capacity(height)` 再 `resize(height, NIL)`，两步操作。

**方案**：直接用 `vec![NIL; height]`。

**难度**：低

---

### 4.7 Skiplist `insert` RNG 参数化 (#12)

**现状**：`insert` 内部调用 `rand::rng()`。可测试性差（确定性测试需传入固定种子 RNG）。

**方案**：将 RNG 作为参数或结构体字段。但 `rand::rng()` 在 rand 0.9 中已优化为 thread-local 引用，实际开销低。

**难度**：低

---

### 4.8 BlockBuilder 前缀压缩 SIMD (#38)

**现状**：`common_prefix_len` 逐字节循环比较。

**方案**：可用 SIMD 加速（`std::simd` 或 crate），长 key 场景收益明显。

**难度**：中（平台差异 + 短 key 不一定更快）

---

### 4.9 `apply_edit` 全量 Clone (#28)

**现状**：`apply_edit` 每次 `clone()` 整个 `[Vec<FileMetaData>; NUM_LEVELS]`。对大量文件的版本有微小开销。

**方案**：只 clone 被修改的层（CoW 模式）。7 层 × 每层几十个文件的 clone 开销可忽略，低优先级。

**难度**：低

---

## 五、测试类

### 5.1 Compaction 快照约束测试

**现状**：有 compaction 基本测试和快照隔离测试，但缺少"compaction 因活跃快照而不丢弃旧版本"的测试。

**方案**：创建快照 → 写入 → compaction → 验证快照仍能读到旧版本。

**难度**：低

---

### 5.2 并发写入压力测试

**现状**：所有测试单线程。没有多线程并发 put/get/delete 测试。

**方案**：多线程并发写入 + 随机 get 验证，检测死锁和竞态。

**难度**：中

---

### 5.3 大 Value 边界测试

**现状**：MemTable 字节阈值刚加入，缺少 value 刚好等于/略超阈值的边界测试。

**方案**：构造 value 让 `approx_bytes` 刚好达到 `memtable_max_bytes`，验证 flush 触发。

**难度**：低

---

## 统计

| 类别 | 数量 |
|------|------|
| 性能类 | 7 |
| 功能类 | 4 |
| 健壮性类 | 5 |
| 代码质量类 | 9 |
| 测试类 | 3 |
| **合计** | **28** |
| 已修复 | 19 |
| 总问题点 | 63（含已修复） |
