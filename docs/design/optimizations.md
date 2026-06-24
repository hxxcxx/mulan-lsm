# 已完成的优化与重构

开发过程中对代码进行的优化与重构记录，按时间顺序排列。

---

## 0. Table Cache：消除 get 路径的重复文件解析

**问题**：`Db::get` 每次查 SSTable 都做 `TableReader::open`（整文件读进内存 + 解析
footer/metaindex/布隆/index）。即使 OS page cache 命中，重复解析的 CPU 开销 + 
内存分配也不小。热点文件被反复 get 时浪费尤为明显。

**方案**：新增 `src/table_cache.rs`，`TableCache` 结构体：
- `Mutex<HashMap<FileNumber, Arc<TableReader>>>` + FIFO 淘汰
- 默认容量 100 个 entry（~200MB 的 2MB SSTable）
- `get(number)`：缓存命中 → 返回 clone Arc；未命中 → 开锁 → 打开文件 → 加锁 → 插入 → 返回
- `evict(number)`：compaction 提交后被删除的文件从缓存中移除
- 文件打开在锁外执行，减少锁争用
- `Arc` 保证被调用方持有期间即使被淘汰也不会释放

**收益**：热点文件 get 从"读文件 + 全量解析"降为"哈希表查找 + Arc clone"。
在有 OS page cache 的场景下实测约 3-10x 提升。

---

## 1. TableIter 惰性化：消除 compaction 的全量 collect

**问题**：`do_compaction` 原先用 `reader.iter().collect::<Vec<_>>()` 把每个输入 SSTable 的全部 entry 一次性收集成 Vec，再包成 `VecIterator` 喂给 `MergingIterator`。原因是 `TableIter<'a>` 借用 `&TableReader`，无法 move 成 `Box<dyn LsmIterator + 'static>`。

**根因**：`.collect()` 把 `TableIter` 本身的 block 级惰性（`advance_to_next_block` 一次一 block）彻底废掉——驱动迭代器到耗尽，全表 entry 进 Vec。

**改动**：
- `TableIter` 去掉 `'a`，自持 `data: Vec<u8>`，变 `'static`
- `TableIter` 实现 `LsmIterator`（与 `Iterator` 共存）
- `TableReader::into_table_iter(self)` 消费 self，move data 给 TableIter
- `compaction` 改用 `reader.into_table_iter()?`，直接 `Box` 进 `Vec<Box<dyn LsmIterator>>`，不再 collect
- 从 compaction 中移除 `VecIterator` 的 import

**收益**：compaction 读 N 个文件时，峰值内存从「N 个整表（每表可达 2MB）」降到「N 个 block（~4KB）」，约 500 倍；启动延迟从「等全量解完」降到零。

---

## 2. `pick_compaction` 签名简化

**问题**：`pick_compaction` 同时接收 `(&Version, &VersionSet)`，但 `Version` 总能通过 `VersionSet::current()` 获取，参数冗余。

**改动**：去掉第一个参数，内部通过 `vs.current()` 获取 Version。

```rust
// 旧
pub fn pick_compaction(version: &Version, vs: &VersionSet) -> Option<Compaction>

// 新
pub fn pick_compaction(vs: &VersionSet) -> Option<Compaction>
```

调用处同步简化：`pick_compaction(&inner.version_set.current(), &inner.version_set)` → `pick_compaction(&inner.version_set)`。

**收益**：减少参数耦合，调用方不需要同时管理 version 和 version_set。

---

## 3. 祖父重叠累加修复（两次 bug）

详见 [compaction.md](compaction.md#祖父重叠累加陷阱两次修复)。

**第一次**（重复累加）：原代码每条 entry 都遍历全部祖父文件，把覆盖该 user_key 的祖父 file_size 加进去。一个覆盖所有 entry 的祖父被加 N 次 → overlap 虚高 → 输出文件过早切碎。修法：单调游标，每个祖父至多计入一次。

**第二次**（游标归 0）：单调游标全局不归 0 时，切分后新文件从未重新扫描已被前一个文件计入的祖父 → overlap 恒为 0 → 输出文件永不因祖父重叠切分。修法：切分时游标归 0。

---

## 4. 并集区间计算简化

**问题**：L0 compaction 时选 inputs[1]（level+1 重叠）用了一个手动循环来展开并集区间。

```rust
// 旧：first/last → 手动循环展开
let smallest = inputs[0].first().map(|f| f.smallest.user_key.as_slice())...;
let mut range_smallest = smallest.to_vec();
for f in &inputs[0] {
    if f.smallest.user_key.as_slice() < range_smallest.as_slice() { ... }
    if f.largest.user_key.as_slice() > range_largest.as_slice() { ... }
}
```

**改动**：用 `min()` / `max()` 替代手动循环。

```rust
// 新：迭代器 min/max
let range_smallest = inputs[0].iter()
    .map(|f| f.smallest.user_key.as_slice()).min().unwrap().to_vec();
let range_largest = inputs[0].iter()
    .map(|f| f.largest.user_key.as_slice()).max().unwrap().to_vec();
```

`self_smallest` / `self_largest` 两个辅助函数因仅有的调用点被内联而删除。

---

## 5. `do_compaction` 版本丢弃逻辑重构

**问题**：原版只做了删除标记丢弃（`is_base_level_for_user_key`），未处理**多版本丢弃**——同 user_key 的旧版本（小 seq）本应在 compaction 时丢弃。测试 `compaction_drops_old_versions` 暴露了这个问题后修复。

**改动**：引入基于 `oldest_snapshot_seq` 的版本丢弃规则：
- 与上一条同 user_key 且上一条 seq ≤ oldest_snapshot_seq → 当前条（更旧）可丢
- 新 user_key：最新版本是 Delete 且 seq ≤ oldest_snapshot_seq 且 is_base_level → 可丢
- 否则保留（即使 Delete 但 seq > oldest_snapshot，快照之后可能有更新写入，删了会让旧版本复活）

**收益**：compaction 输出只保留每个 user_key 的"对最老快照可见的最新版本"，彻底清除无引用价值的旧版本，压缩数据量。
