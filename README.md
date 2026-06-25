# mulan-lsm

一个从零实现的 LSM-Tree 存储引擎（Rust），完整实现了 LevelDB 的核心架构。
适合学习 LSM 原理、Rust 系统编程的参考实现。

## 功能

- **内存 KV**：MemTable + SkipList，多版本 + 删除标记
- **磁盘格式**：SSTable（Block 前缀压缩 + 布隆过滤器 + 稀疏索引）+ WAL 预写日志
- **版本管理**：Manifest + VersionSet + CURRENT 原子切换 + 崩溃恢复
- **Compaction**：size-tiered 触发 + round-robin 选文件 + grandparent 切分控制 + 快照感知丢弃
- **MVCC**：快照一致性读（SnapshotGuard RAII）+ 范围扫描（DBIter）
- **Table Cache**：LRU 缓存已打开的 SSTable，减少 get 路径重复解析

## 架构

```
put/get/delete → Db
                  ├─ MemTable (SkipList)          可写内存层
                  ├─ WAL (预写日志)               崩溃恢复保障
                  ├─ SSTable (只读)               磁盘有序表
                  │    ├─ Block (前缀压缩)        4KB 存储单元
                  │    ├─ Bloom Filter            概率过滤，减少无效 IO
                  │    └─ Index Block             稀疏索引，O(log n) 定位
                  ├─ TableCache (LRU)            缓存已打开的 SSTable
                  ├─ VersionSet (版本管理)        Manifest + CURRENT
                  └─ Compaction (后台归并)        多层归并 + 空间回收
```

## 快速开始

```rust
use mulan_lsm::db::{Db, Options};

let db = Db::open(&dir, Options::default())?;

// 基本读写
db.put(b"key", b"value")?;
let val = db.get(b"key")?;           // Some(b"value")
db.delete(b"key")?;
let val = db.get(b"key")?;           // None

// 快照一致性读
let snap = db.new_snapshot();
db.put(b"key", b"new")?;
let old = db.get_at(&snap, b"key")?; // Some(b"value") — 快照隔离

// 范围扫描
let (_snap, iter) = db.new_iterator()?;
for (k, v) in iter {
    println!("{}: {}", String::from_utf8_lossy(&k), String::from_utf8_lossy(&v));
}
```

## 文档

- [设计文档](docs/design/README.md) — 按模块组织的设计原理与历史教训
- [待改进项](docs/design/pending-improvements.md) — 已知优化方向

## 构建与测试

```bash
cargo build --release
cargo test          # 213 个测试全部通过
cargo test -- --ignored  # 含压力测试
```

## 许可证

MIT
