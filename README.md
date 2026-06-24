# mulan-lsm

一个从零实现的 LSM-Tree 存储引擎（Rust），完整实现了 LevelDB 的核心架构。
适合学习 LSM 原理、Rust 系统编程的参考实现。

## 项目状态

M1~M6 全部完成，**201 个测试通过**。

- **内存 KV**：MemTable + SkipList
- **磁盘格式**：SSTable (Block/Filter/Index) + WAL
- **版本管理**：Manifest + VersionSet + 崩溃恢复
- **Compaction**：size-tiered + round-robin + grandparent 切分
- **MVCC**：快照一致性读 + 范围扫描

## 架构

```
put/get/delete → Db
                  ├─ MemTable (SkipList)
                  ├─ WAL (预写日志)
                  ├─ SSTable (只读)
                  ├─ VersionSet (版本管理)
                  └─ Compaction (后台归并)
```

## 快速开始

```rust
use mulan_lsm::db::{Db, Options};

let db = Db::open(&dir, Options::default())?;
db.put(b"key", b"value")?;
let val = db.get(b"key")?;

// 快照读
let snap = db.new_snapshot();
let val = db.get_at(&snap, b"key")?;

// 范围扫描
let (_snap, iter) = db.new_iterator()?;
for (k, v) in iter { ... }
```

## 文档

- [设计文档](docs/design/README.md) — 按模块组织的设计原理与历史教训

## 构建与测试

```bash
cargo build --release
cargo test      # 201 个测试全部通过
```

## 许可证

MIT
