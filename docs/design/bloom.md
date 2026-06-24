# Bloom Filter — 概率过滤

**核心性质**：零假阴性（说"不在"一定不在），有假阳性（说"可能在"可能误判）。

**双哈希技巧**

```rust
fn double_hash(key: &[u8]) -> (u32, u32) {
    let h = bloom_hash(key);        // 一次基础哈希
    let delta = (h >> 17).max(1);   // 从高位取增量
    (h, delta)
}
```

第 i 次哈希 = `h + i * delta`（wrapping）。只算一次完整哈希，派生 k 个，比算 k 次快得多。

**哈希函数设计**

```rust
fn bloom_hash(data: &[u8]) -> u32 {
    let mut h: u32 = data.len() as u32;
    for &b in data {
        h = h.wrapping_mul(0x0001_9360_u32);
        h ^= b as u32;
    }
    h.wrapping_mul(0x9E37_79B1)
}
```

不要密码学强度（如 SHA），只需要：确定性、分布均匀、快。

**在 SSTable 中的位置**

```
TableReader::get_entry
  → 布隆过滤（~1KB 内存）→ 省掉 99% 不必要的 data block IO
  → 布隆通过 → index 二分 + data block 查找
```
