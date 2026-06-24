# DB — 打开与恢复

## 基于 seq 的 MVCC（M6）

mulan-lsm 的 MVCC 完全围绕 `seq`（单调递增序列号）构建：

- **写分配 seq**：每次 `put`/`delete` 写入 MemTable 时，`MemTable::sequence()` 推进 seq。写入后立即同步到 `VersionSet.last_sequence`，使 seq 成为单一权威源
- **快照固定 seq**：`new_snapshot()` 取当前 `last_sequence` 并注册。后续 `get_at(&snap, key)` 用该 seq 过滤——只看到 seq ≤ 快照 seq 的版本，看不到之后写入
- **读按 seq 过滤**：所有读路径（MemTable 哨兵 SSTable lookup 使用 snapshot_seq）都用 InternalKey 的 seq 比较做快照一致性读
- **Compaction 以 oldest snapshot 决定 GC 边界**：`run_one_compaction` 取 `VersionSet::oldest_snapshot_seq()`，只有 seq ≤ 该值的旧版本才能安全丢弃。快照存在期间 compaction 不回收其引用的旧版本

**create_new（新库）**：
```
IdGenerator(1) → manifest(000001) → WAL(000002)
manifest: set_log_number=2, set_next_file_number=3, set_last_sequence=0
write_current → 写 CURRENT
```

**recover_open（恢复）**：
```
VersionSet::recover(dir) → 回放 manifest 重建 VersionSet
回放当前 WAL 重建 MemTable
如果 WAL 有数据 → flush 成 SSTable → 切换新 WAL
remove_obsolete_files → 清理孤儿文件
```

**remove_obsolete_files**：只保留 live SSTable + 当前 WAL + 当前 manifest。孤儿文件（compaction 替换掉的旧文件、崩溃残留）全部清理。

**并发模型**：`Arc<(Mutex<DbInner>, Condvar)>` — 前台 `put`/`delete` 持锁写 MemTable + WAL，`get` 持锁取 `Arc<Version>` 快照后释放锁无锁查 SSTable，后台线程持锁 compaction。
