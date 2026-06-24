# mulan-lsm 设计文档

按模块组织的设计原理、权衡与历史教训。

| 模块 | 核心问题 | 文件 |
|------|---------|------|
| [InternalKey](internal-key.md) | 多版本排序、Ord 与字节布局解耦 | `src/internal_key.rs` |
| [SkipList](skiplist.md) | 并发安全的跳表、`find_prev` 的三种消费者 | `src/skiplist.rs` |
| [MemTable](memtable.md) | 区分 Delete 与 NotFound 的读路径 | `src/memtable.rs` |
| [Block](block.md) | 前缀压缩、restart point 二分 | `src/sstable/block.rs` |
| [SSTable](table.md) | 文件布局、pending_index、5 步查找 | `src/sstable/table.rs` |
| [Bloom Filter](bloom.md) | 双哈希、零假阴性 | `src/bloom.rs` |
| [WAL](wal.md) | 32KB block 分片、崩溃恢复 | `src/wal.rs` |
| [Varint](varint.md) | 整数压缩、定长/变长混用 | `src/varint.rs` |
| [File Meta](file-meta.md) | 文件命名规则、IdGenerator | `src/file_meta.rs` |
| [Manifest](manifest.md) | tag-based VersionEdit、CURRENT 原子写 | `src/manifest.rs` |
| [Version](version.md) | 不可变快照、Arc 共享、get_overlaps | `src/version.rs` |
| [DB](db.md) | create_new / recover_open、孤儿清理 | `src/db.rs` |
| [Iterator](iterator.md) | MergingIterator 归并、DBIter 过滤 | `src/iterator.rs` |
| [Compaction](compaction.md) | 触发、选文件、祖父控制、锁粒度 | `src/compaction.rs` |
| [通用原则](principles.md) | 错误处理、边界测试、设计哲学 | — |
| [已完成的优化](optimizations.md) | TableIter 惰性化、API 简化、bug 修复 | — |
