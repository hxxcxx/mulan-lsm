# WAL — 预写日志

**Record 格式**

```
[checksum(4)][length(2)][type(1)][data(length)]
```

Type 四种：FULL(1) / FIRST(2) / MIDDLE(3) / LAST(4)

**Block（32KB chunk）概念的由来**

WAL 文件不是连续字节流，而是切成 32KB 的固定"格子"。作用是崩溃恢复时快速定位：遇到损坏（crc 不通过）直接停止；遇到 trailer（全 0 填充）跳到下个 block 边界继续。

```
[block 0: 32KB][block 1: 32KB]...
```

block 末尾不足 7 字节（放不下一个 record 头）时，填 0 trailer 补齐到 block 边界。

**`add_record` 的分片逻辑**

```
记录大小 ≤ 32761 字节 → FULL
记录填满 block 还有剩余 → FIRST + ... + LAST（跨 block 时自动切分片）
```

**崩溃语义**

`add_record` 只是 `write_all`（写到页缓存），不保证落盘。`sync()` 才是 fsync。
- 写入后 fsync 前崩溃 → 那笔数据可能丢
- fsync 后崩溃 → 数据安全
- 恢复时 WalReader 遇到 crc 不通过 → 停，返回完整记录
- 收到一条 First 但没收到对应的 Last → fragment 视为残缺，丢弃

Key design: **crc32c 覆盖 type + data**，防止 type 被篡改（改 type 会导致拼装错误但 crc 不通过）。

**Manifest 也复用 WAL 格式**

`ManifestWriter` 内部 wrapping `WalWriter`，每个 `VersionEdit` 序列化后通过 `add_record` 追加。
