# File Meta — 文件命名

```
000001.sst        SSTable
000002.log        WAL
MANIFEST-000003   Manifest
CURRENT           指向当前 manifest 的纯文本文件
CURRENT.dbtmp     原子写 CURRENT 的中转文件
```

FileNumber 是 newtype 包装 `u64`，和普通整数类型不混用。

`IdGenerator` 单调递增，文件编号永不复用。
