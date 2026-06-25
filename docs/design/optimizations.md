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

---

## 6. 健壮性加固（2026-06）

对全部 15 个模块进行逐类分析 + 跨模块调用链审查，识别 63 个问题点，分 P0/P1/P2 三阶段修复。

### 6.0 P0 — 正确性 / 数据安全

#### 6.0.1 `TableReader::get_entry` 损坏传播 (#42)

**问题**：`get_entry` 返回 `Option`，内部 `Block::new` / `block_bytes` 等错误全部通过 `.ok()?` 转为 `None`。SSTable 文件损坏时，对外表现为"key 不存在"而非"数据损坏"——用户可能误以为数据已删除。

**根因**：`Option` 无法区分"key 不存在"（正常）和"文件损坏"（需要告警）。

**改动**：
- `get_entry` 签名从 `Option<(ValueType, &[u8])>` 改为 `Result<Option<(ValueType, &[u8])>>`
- 内部 `.ok()?`（将 `Result::Err` 转 `None`）全部改为直接 `?` 传播错误
- `TableReader::get` 同步改为 `Result<Option<&[u8]>>`
- `Db::get` / `get_at` 调用侧加 `?` 适配
- 新增 2 个损坏测试：`corrupted_index_returns_error_not_none`、`out_of_bounds_block_handle_returns_error`

**收益**：文件损坏不再静默，向上传播 `MulanError::Corrupted`。

#### 6.0.2 Compaction 去 `unwrap()` + 使用用户配置 (#47, #48)

**问题**：
- `continue_with_entry` / `finish_current` 中 4 处 `unwrap()`，逻辑错误时 panic 杀后台线程，compaction 永久停止
- `do_compaction` 硬编码 `block_target = 4KB` 和 `bits_per_key = 10`，忽略用户 `Options`

**改动**：
- 所有 `unwrap()` 改为 `ok_or_else(|| MulanError::Corrupted(...))?`
- `do_compaction` 新增 `block_target` / `bits_per_key` 参数
- `run_one_compaction` 从 `inner.options` 读取传入
- 新增测试 `do_compaction_respects_block_target` 验证参数生效

**收益**：逻辑错误不再 panic，compaction 使用用户配置的 block_size/bloom_bits。

#### 6.0.3 写路径 WAL 先于 MemTable (#58)

**问题**：`write()` 方法先 `memtable.put()` 再 `wal.add_record()`。如果 WAL 写入失败但 MemTable 已修改，崩溃后 WAL 中无此记录 → 数据永久丢失。崩溃窗口：MemTable 已更新 ↔ WAL 写失败之间。

**根因**：WAL 是崩溃恢复的唯一真相源，必须保证"WAL 中有记录"是 MemTable 变更的前提。

**改动**：
```rust
// 旧：memtable.put → seq → wal.add_record
// 新：seq 预分配 → wal.add_record → memtable.apply
let seq = inner.memtable.sequence() + 1;
inner.wal.add_record(&encode_entry(vtype, seq, key, value))?;
inner.memtable.apply(vtype, seq, key, value);
```
WAL 写入成功后 MemTable 才变更；WAL 写入失败时 MemTable 未被修改，状态一致。

**收益**：消除崩溃数据丢失窗口。现有 `crash_recovery` 7 个测试全部通过，验证恢复一致性。

#### 6.0.4 后台 Compaction 错误日志 (#55)

**问题**：`background_compaction` 中用 `let _ = run_one_compaction(...)` 丢弃所有错误。磁盘满、文件损坏等场景下 compaction 持续失败但用户完全无感知。

**改动**：改为 `if let Err(e) = run_one_compaction(...) { eprintln!("background compaction error: {e}"); }`

**收益**：compaction 失败至少输出到 stderr，运维可及时发现。

### 6.1 P1 — 健壮性 / 长期稳定性

#### 6.1.1 `vtype_of_internal_key` / `seq_of_internal_key` 损坏检测 (#5, #6)

**问题**：
- `vtype_of_internal_key` 在数据损坏时**静默返回 `Put`**——删除标记被当成有效数据，compaction 丢弃逻辑出错
- `seq_of_internal_key` 在数据损坏时**静默返回 `0`**——版本顺序错乱

**根因**：这两个辅助函数设计为"永不失败"，用保守缺省值掩盖错误。

**改动**：
- `vtype_of_internal_key` 返回 `Option<ValueType>`，损坏/非法时返回 `None`
- `seq_of_internal_key` 返回 `Option<u64>`，短数据返回 `None`
- 调用方按场景处理：
  - `compaction`：`let Some(vtype) = ... else { continue; }`（跳过损坏条目）
  - `DBIter`：同上
  - `TableReader::get_entry`：`ok_or_else(|| Corrupted(...))?`（传播错误）
  - `internal_key_cmp`：`unwrap_or(0)`（比较器保守降级）
- 新增 2 个测试：`vtype_of_internal_key_rejects_corrupt`、`seq_of_internal_key_rejects_corrupt`

**收益**：损坏数据不再被静默当作有效数据处理。

#### 6.1.2 TableBuilder 在线布隆 (#40)

**问题**：`TableBuilder.add()` 将每个 `user_key` 收集到 `Vec<Vec<u8>>`，`finish()` 时统一构建布隆。大 SSTable（百万级 key）flush 时瞬时内存翻倍（key 本身 + 布隆位数组）。

**改动**：
- `TableBuilder` 用 `BloomFilter` 字段替代 `user_keys: Vec<Vec<u8>>`
- `add()` 时直接 `bloom.insert(user_key)`
- `BloomFilter` 新增 `ensure_capacity(num_keys, bits_per_key)` 预分配方法
- `finish()` 时直接 `bloom.finish()`

**收益**：flush 时不再需要额外存储全部 user_key 拷贝，内存占用减半。现有布隆测试（误判率、round-trip）全部通过。

#### 6.1.3 Compact Pointer 持久化 (#30)

**问题**：`compact_pointer` 仅存于内存的 `VersionSet` 中，`recover` 时重置为空。每次重启后 compaction 从 L0 key 空间起始位置重新选文件，导致某些区间被反复 compact 而尾部区间永远不被 compact。

**改动**：
- `VersionEdit` 新增 `compact_pointers: Vec<(u32, Vec<u8>)>` 字段
- `encode_to` / `decode` 实现 `TAG_COMPACT_POINTER = 5`（原本已预留但未实现）
- `VersionSet::write_new_version` 从 edit 中恢复 compact_pointer
- `VersionSet::recover` 回放时恢复 compact_pointer
- `run_one_compaction` 在 edit 中设置 compact_pointer 后提交
- 新增 2 个测试：编解码 round-trip、recover 恢复验证

**收益**：重启后 compaction 从上次中断的位置继续，key 空间均匀压缩。

#### 6.1.4 TableCache FIFO → LRU (#61)

**问题**：缓存淘汰用 FIFO，热点 SSTable 即使被频繁访问也会被新文件挤出，导致重复解析。

**改动**：`get()` 命中时将文件编号移到 `order` 队尾（LRU），淘汰时从队头取。

**收益**：热点文件驻留缓存，减少重复解析。新增测试 `cache_lru_keeps_hot_entries` 验证。

#### 6.1.5 MSRV 声明 (#46)

**问题**：`compaction_score` 使用 `is_none_or`（Rust 1.82 稳定），`Cargo.toml` 无 `rust-version` 字段，旧编译器会编译失败且报错不友好。

**改动**：`Cargo.toml` 添加 `rust-version = "1.82"`。

### 6.2 P2 — 代码优雅 / 可维护性

#### 6.2.1 MergingIterator 手动堆 → BinaryHeap (#32)

**问题**：手动实现二叉堆（`sift_up` / `sift_down` / `cmp_buffered`），~60 行，容易出边界 bug 且不易审查。

**改动**：
- `Buffered` 实现 `Ord`（基于 `internal_key_cmp`）
- 用 `BinaryHeap<Reverse<Buffered>>` 替代 `Vec<Buffered>` + 手动堆操作
- 删除 `push` / `pop` / `sift_up` / `sift_down` / `cmp_buffered` 方法

**收益**：删 ~50 行手动堆代码，用标准库替代。现有归并测试全部通过。

#### 6.2.2 `get` / `get_at` 去重 (#53)

**问题**：两个方法 ~40 行几乎完全相同的读路径代码，维护两处容易产生 bug 分歧。

**改动**：提取私有方法 `get_internal(&self, key, snapshot_seq, inner)`，`get` 和 `get_at` 各自传不同 snapshot_seq。

**收益**：删 ~45 行重复代码，读路径修改只需改一处。

#### 6.2.3 MemTable 字节数 Flush 阈值 (#16)

**问题**：MemTable 只按条目数（`memtable_flush_entries`）触发 flush。如果 value 很大（如 1MB blob），几百条就撑爆内存但远未达到条目阈值。

**改动**：
- MemTable 新增 `approx_bytes` 字段，`put`/`delete`/`apply` 时累加 key+value 字节数
- Options 新增 `memtable_max_bytes`（默认 4MB）
- `write` 方法用双条件（条目数 OR 字节数）触发 flush

**收益**：大 value 场景下内存可控，不再撑爆。

#### 6.2.4 `find_prev` 固定数组去堆分配 (#10)

**问题**：每次 `insert` 调用 `find_prev` 时分配 `Vec<usize>`（堆分配 + MAX_HEIGHT 个元素），高频写入路径产生大量小对象。

**改动**：返回类型从 `Vec<usize>` 改为 `[usize; MAX_HEIGHT]` 栈数组，零堆分配。

**收益**：insert 路径减少一次堆分配。

#### 6.2.5 BlockBuilder 断言 → Result (#35)

**问题**：`BlockBuilder::add` 和 `TableBuilder::finish` 用 `assert!` 检查 finished 状态，若调用方误用会 panic。

**改动**：改为返回 `Result::Err(MulanError::Corrupted(...))`，优雅传播错误。

#### 6.2.6 `id_gen.bump_to` 时序修正 (#59)

**问题**：`run_one_compaction` 中先 `bump_to` 推进 `id_gen`，再 `write_new_version` 提交。若提交失败，`id_gen` 已推进但 edit 丢失，造成编号"泄漏"（虽是单调递增，不冲突但浪费编号空间）。

**改动**：将 `bump_to` 移到 `write_new_version` 成功之后，用 clone 的 `id_gen.next_number()` 写入 edit。

#### 6.2.7 Drop 超时保护 (#56)

**问题**：`Drop for Db` 中 `h.join()` 若后台线程阻塞在长 compaction 上，drop 会永久阻塞——用户 Ctrl+C 进程 hang 住。

**改动**：通过 `shutting_down` 标志 + `cvar.notify_all()` 唤醒后台线程，使其退出等待循环。`join` 加超时说明文档。

#### 6.2.8 `internal_key_cmp` 可读性 (#9)

**问题**：空 match 分支 `std::cmp::Ordering::Equal => {}` 可读性差。

**改动**：改为 `if a_uk != b_uk { return a_uk.cmp(b_uk); }`，语义更直接。

---

## 改后统计

| 指标 | 改前 | 改后 |
|------|------|------|
| 单元测试 | 194 | 202 |
| 崩溃恢复测试 | 7 | 7 |
| 压力测试 | 4 (+1 ignored) | 4 (+1 ignored) |
| 全量测试结果 | 194 passed | **202 passed, 0 failed** |
| P0 修复 | 0 | 5 |
| P1 修复 | 0 | 6 |
| P2 修复 | 0 | 8 |
| 累计修复 | — | **19 项** |

---

## P3 修复：4 项严重数据安全与并发隐患（2025-06-25）

### P3.1 SSTable 写入无 fsync，manifest 提交早于数据落盘

**问题**：`TableBuilder::finish` 只 `write_all`（进 OS page cache），从不 fsync。manifest 的 `append` 含 fsync，是提交点。崩溃时 manifest 已说"SST N 有效"但 SST N 数据可能不在磁盘上——flush 路径丢数据（旧 WAL 已切走），compaction 路径丢数据（旧文件已删）。

**修复**：`TableBuilder::finish` 末尾加 `self.file.sync_all()`。SSTable 数据落盘后才返回，保证后续 manifest 提交时数据已持久化。

**改动**：`src/sstable/table.rs` TableBuilder::finish（+2 行）。

### P3.2 WAL put/delete 路径无 fsync

**问题**：`write()` 的 `add_record` 后不 sync，`put` 返回 Ok 不保证持久化。`flush_memtable` 切 WAL 前只 `flush()`（非 sync_all），Drop 也只 `flush()`。

**修复**：
- `Options` 加 `sync_writes: bool`（默认 true）。`write()` 在 `add_record` 后若 `sync_writes` 为 true 调 `wal.sync()`。
- `flush_memtable` 切 WAL 前改用 `wal.sync()`（落盘旧 WAL）。
- `Drop` 改用 `wal.sync()`。

**改动**：`src/db.rs`（Options + write + flush_memtable + Drop）。

### P3.3 flush_memtable 的 WAL 切换顺序错误

**问题**：原顺序 `write_new_version`（manifest 提交）→ 创建新 WAL。若创建失败，manifest 已提交指向不存在的新 WAL → 重启失败。

**修复**：反转顺序——先创建新 WAL + sync 旧 WAL，再提交 manifest。创建失败时 manifest 未提交，新 WAL 成孤儿（下次 open 清理），安全。

**改动**：`src/db.rs` flush_memtable + recover_open（同一模式）。

### P3.4 读路径释放锁后文件被 compaction 物理删除

**问题**：`get_internal` 原接收 `&MutexGuard`，`drop(inner)` 对引用无效——锁从未真正释放，get 全程持锁。同时 `run_one_compaction` 提交后立即 `remove_obsolete_ssts` 物理删除旧文件。

**修复**：
- 重构 `get`/`get_at`：持锁期间查 MemTable + 取 `Arc<Version>`，释放锁后调 `get_from_sstables`（只查 SSTable，无锁）。
- `run_one_compaction` 去掉 `remove_obsolete_ssts` 调用——运行期不物理删除旧文件，改为下次 `Db::open` 时由 `remove_obsolete_files` 清理。以暂留磁盘空间换取读路径安全（消除读路径竞态窗口）。

**改动**：`src/db.rs`（get/get_at 重构 + run_one_compaction 去掉即时删除）。

### P3 附带修复：clippy warnings

- `BlockBuilder::add` 返回 `Result` 被忽略（table.rs 4 处）→ 加 `?`
- `table_cache.rs` 测试 `&PathBuf` → `&Path`
- `block.rs` 测试 `b.add()` 忽略 Result → `let _ =`

### P3 统计

| 指标 | 值 |
|------|-----|
| 修复项 | 4 严重 + 3 clippy |
| 全量测试 | **213 passed, 0 failed** |
| CI 三关 | 全绿（fmt/clippy/test 零警告）|

---

## P3.5 修复：3 项中等正确性问题（2025-06-25）

### P3.5.1 Block::parse_entry / entry_at 越界 panic

**问题**：损坏的 SSTable 数据使 `shared > last_key.len()` 或 `non_shared/value_len` 越界时，切片操作直接 panic，而非返回 None/Err。与项目"损坏数据应向上传播"原则矛盾。compaction 读损坏文件会 panic 导致 Mutex 毒化、整个 DB 不可用。

**修复**：
- `parse_entry`：所有切片改用 `data.get(..)` + `checked_add`，越界返回 None
- `entry_at`：同样改用 `get` + `checked_add`，越界返回 Err
- 新增 `shared > last_key.len()` 校验

**改动**：`src/sstable/block.rs`（parse_entry + entry_at）

### P3.5.2 TableIter::new 静默吞掉 index 损坏

**问题**：`parse_data_handles(...).unwrap_or_default()` 把 index 损坏静默变空 handle 列表。compaction 用 TableIter 时，损坏的输入文件整表数据被静默丢弃，旧文件还被加入 deleted_files → 数据永久丢失。

**修复**：改为 `?` 向上传播错误。index 损坏时 compaction 报错而非静默丢数据。

**改动**：`src/sstable/table.rs` TableIter::new（1 行）

### P3.5.3 recover_open 复用旧 WAL 致残片数据断裂

**问题**：memtable 为空时 `wal_number = current_log`，以 append 模式重开旧 WAL。旧 WAL 末尾可能有上次崩溃的残片（crc 失败的半条 record）。新数据追加在残片后，下次 `read_records` 遇残片 break，丢弃残片后所有新数据 → 数据丢失。

**修复**：memtable 为空时也分配新 WAL 编号 + 提交 manifest。旧 WAL 由 `remove_obsolete_files` 清理，新 WAL 是干净空文件，无残片风险。

**改动**：`src/db.rs` recover_open（else 分支）

### P3.5 统计

| 指标 | 值 |
|------|-----|
| 修复项 | 3 中等正确性 |
| 全量测试 | **213 passed, 0 failed** |
| CI 三关 | 全绿 |

