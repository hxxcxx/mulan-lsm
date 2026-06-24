# Manifest — 版本变更日志

**VersionEdit 序列化**：手写 tag-based 格式，每个字段有独立 tag 值。兼容性：加新字段只需定义新 tag，旧版跳过未知 tag。

已定义的 tag：

| Tag | 字段 | 编码 |
|-----|------|------|
| 1 | `comparator_name` | 定长字符串（length + bytes） |
| 2 | `log_number` | varint64 |
| 3 | `next_file_number` | varint64 |
| 4 | `last_sequence` | varint64 |
| 5 | `new_file` | (level(varint) + file_size(varint) + internal_key(ik) + internal_key(ik)) |
| 6 | `deleted_file` | (level(varint) + file_number(varint)) |

每条 edit 包含若干 tag-value 对尾部无终止标记——解析到输入流结尾即结束。

**恢复流程**：打开 CURRENT → 读 manifest 文件 → WalReader 扫描所有 edit record → 逐个 apply 累积成 Version。apply 时新文件加到指定 level、删除文件从本层移除。恢复完的 Version 里的文件编号无冲突（next_file_number 保证单调递增）。

**CURRENT 原子写**：`write(CURRENT.dbtmp)` → `fsync` → `rename(CURRENT.dbtmp, CURRENT)`。Windows 下 `std::fs::rename` 可覆盖已存在目标。

**Manifest 复用 WAL 格式**：内部 wrap `WalWriter`/`WalReader`，每条 VersionEdit 序列化后以 WAL record 形式写入。复用带来 crc32c 校验、故障后停在最后一个完整 edit 等保护。

