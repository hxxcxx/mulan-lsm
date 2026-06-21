//! Bloom Filter：概率数据结构，快速判定 key 是否"可能"在集合里。
//!
//! 用于 SSTable 读路径：读 data block 前先问布隆，若说"不在"则 100% 不在，省一次读。
//! 特性：零假阴性（说不在我信），有假阳性（说在我不信，可能误判）。

/// Bloom Filter。
pub struct BloomFilter {
    /// 位数组（以字节存储，bit 紧凑）。
    bits: Vec<u8>,
    /// 哈希函数个数。
    k: u8,
}

impl BloomFilter {
    /// 按 bits_per_key 创建空 filter，并据此确定 k。
    /// k = max(1, round(bits_per_key * ln(2)))，是给定 bits_per_key 下的最优哈希数。
    /// bits_per_key=10 → k=7。
    pub fn new(bits_per_key: usize) -> Self {
        let k = ((bits_per_key as f64) * std::f64::consts::LN_2)
            .round()
            .max(1.0) as u8;
        let k = k.min(30); // 上限保护，过多哈希反增误判。
        BloomFilter {
            bits: Vec::new(),
            k,
        }
    }

    /// 用给定的 key 列表构造 filter（批量插入，按总 bits 预分配）。
    pub fn from_keys(keys: &[&[u8]], bits_per_key: usize) -> Self {
        let total_bits = (keys.len() * bits_per_key).max(64);
        // 向上对齐到字节。
        let total_bytes = total_bits.div_ceil(8);
        let mut bf = BloomFilter::new(bits_per_key);
        bf.bits = vec![0u8; total_bytes];
        for key in keys {
            bf.insert(key);
        }
        bf
    }

    /// 插入一个 key。必须在 finish 前调用。
    pub fn insert(&mut self, key: &[u8]) {
        let nbits = (self.bits.len() * 8) as u32;
        if nbits == 0 {
            return;
        }
        let (h, delta) = double_hash(key);
        let mut h = h;
        for _ in 0..self.k {
            // % nbits 把哈希映射到位数组范围内。
            let bitpos = (h % nbits) as usize;
            self.bits[bitpos / 8] |= 1u8 << (bitpos % 8);
            h = h.wrapping_add(delta);
        }
    }

    /// 查询 key 是否"可能"在集合里。false = 肯定不在；true = 可能在（可能误判）。
    pub fn may_contain(&self, key: &[u8]) -> bool {
        let nbits = (self.bits.len() * 8) as u32;
        if nbits == 0 {
            return false;
        }
        let (h, delta) = double_hash(key);
        let mut h = h;
        for _ in 0..self.k {
            let bitpos = (h % nbits) as usize;
            if self.bits[bitpos / 8] & (1u8 << (bitpos % 8)) == 0 {
                // 任一位为 0，则肯定未插入过。
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }

    /// 序列化为字节：[位数组] + [k]。供 SSTable meta block 存储。
    pub fn finish(mut self) -> Vec<u8> {
        self.bits.push(self.k);
        self.bits
    }

    /// 从字节（finish 输出的格式）重建 filter。
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.is_empty() {
            return None;
        }
        let k = data[data.len() - 1];
        let bits = data[..data.len() - 1].to_vec();
        Some(BloomFilter { bits, k })
    }
}

/// 双哈希技巧：只算一次基础哈希，派生 k 个哈希值用等差数列。
/// 返回 (基础哈希 h, 公差 delta)。第 i 个哈希 = h + i * delta（wrapping）。
/// LevelDB 用 delta = h >> 17（高位），这里用 HASH_BITS/2 的位置取高位。
fn double_hash(key: &[u8]) -> (u32, u32) {
    let h = bloom_hash(key);
    let delta = (h >> 17).max(1);
    (h, delta)
}

/// LevelDB 风格的简单哈希：位移 + 乘法混合。确定性、跨平台一致、分布均匀。
fn bloom_hash(data: &[u8]) -> u32 {
    let mut h: u32 = data.len() as u32;
    for &b in data {
        h = h.wrapping_mul(0x0001_9360_u32);
        h ^= b as u32;
    }
    // 额外混淆，改善分布。
    h.wrapping_mul(0x9E37_79B1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_false_negatives() {
        // 插入的 key 必须全部 may_contain=true（零假阴性，布隆的核心保证）。
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| format!("k{i}").into_bytes()).collect();
        let keys_ref: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
        let bf = BloomFilter::from_keys(&keys_ref, 10);
        for k in &keys {
            assert!(bf.may_contain(k), "false negative on {:?}", k);
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        // 插入 1000 个 key，查另外 1000 个未插入的 key，误判率应远低于 50%。
        let inserted: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("in-{i}").into_bytes())
            .collect();
        let inserted_ref: Vec<&[u8]> = inserted.iter().map(|v| v.as_slice()).collect();
        let bf = BloomFilter::from_keys(&inserted_ref, 10);

        let absent: Vec<Vec<u8>> = (0..1000u32)
            .map(|i| format!("out-{i}").into_bytes())
            .collect();
        let mut false_positives = 0;
        for k in &absent {
            if bf.may_contain(k) {
                false_positives += 1;
            }
        }
        let rate = false_positives as f64 / 1000.0;
        // bits_per_key=10 理论误判率约 0.82%。放宽到 5%，容忍实现差异。
        assert!(
            rate < 0.05,
            "false positive rate too high: {false_positives}/1000 = {rate}"
        );
    }

    #[test]
    fn finish_round_trip() {
        let keys: Vec<Vec<u8>> = (0..200u32).map(|i| format!("k{i}").into_bytes()).collect();
        let keys_ref: Vec<&[u8]> = keys.iter().map(|v| v.as_slice()).collect();
        let bf = BloomFilter::from_keys(&keys_ref, 10);
        let bytes = bf.finish();
        let restored = BloomFilter::from_bytes(&bytes).unwrap();
        // round-trip 后查询结果一致。
        for k in &keys {
            assert!(restored.may_contain(k));
        }
        assert!(!restored.may_contain(b"definitely-absent"));
    }

    #[test]
    fn empty_filter_says_no() {
        // 空 filter（无任何插入）对任意 key 都返回 false。
        let bf = BloomFilter::new(10);
        // 空 bits 时 may_contain 返回 false。
        assert!(!bf.may_contain(b"any-key"));
    }

    #[test]
    fn from_bytes_rejects_empty() {
        assert!(BloomFilter::from_bytes(&[]).is_none());
    }

    #[test]
    fn deterministic_hash() {
        // 相同 key 多次哈希结果一致（确定性，跨运行稳定）。
        let k = b"stable-key";
        assert_eq!(bloom_hash(k), bloom_hash(k));
        // 不同 key 哈希不同（概率上几乎必然）。
        assert_ne!(bloom_hash(b"a"), bloom_hash(b"b"));
    }

    #[test]
    fn k_value_for_bits_per_key() {
        // bits_per_key=10 → k ≈ 0.69*10 = 6.9 → 7。
        let bf = BloomFilter::new(10);
        assert!(bf.k >= 5 && bf.k <= 8, "k={}", bf.k);
    }
}
