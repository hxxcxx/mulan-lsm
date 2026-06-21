//! InternalKey：把 user_key 扩展成 (user_key, seq, vtype)，用一个包装类型
//! 同时承载"多版本"和"删除"。排序由手动实现的 Ord 决定，不依赖字节字典序。

use crate::error::{MulanError, Result};

/// 序列号最大值。原版保留 u64::MAX 作为查找哨兵，实际使用的 seq 上限比它小 1。
pub const MAX_SEQUENCE: u64 = u64::MAX - 1;

/// InternalKey 序列化后的末尾固定 9 字节：8 字节 sequence + 1 字节 type。
const FOOTER_LEN: usize = 9;
const SEQ_LEN: usize = 8;
const TYPE_LEN: usize = 1;

/// 写入操作的类型。Delete 不是真删除，而是写入一个删除标记。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Put = 0,
    Delete = 1,
}

impl ValueType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ValueType::Put),
            1 => Some(ValueType::Delete),
            _ => None,
        }
    }
}

/// InternalKey 包装类型。排序规则（手动 Ord）：
///   1. user_key 字节字典序升序
///   2. 同 user_key 下 seq 降序（大 seq 在前，让最新版本最先被查找命中）
///   3. seq 也相同时按 vtype 排序（理论不发生，仅为 Ord 全序完备）
///
/// 排序完全由 Ord 决定，seq 的字节布局（encode 时用小端）不参与排序。
#[derive(Clone, Debug)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub vtype: ValueType,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq: u64, vtype: ValueType) -> Self {
        InternalKey {
            user_key,
            seq,
            vtype,
        }
    }

    /// 序列化为字节：`user_key + seq(小端) + type`。
    /// 仅供 WAL/SSTable 持久化用；排序由 Ord 决定，与此字节布局无关。
    /// 字节序选小端仅为惯例（与 WAL/SSTable 的整数一致），不影响正确性。
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.user_key.len() + FOOTER_LEN);
        buf.extend_from_slice(&self.user_key);
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.push(self.vtype as u8);
        buf
    }

    /// 反序列化。返回拥有的 InternalKey。
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < FOOTER_LEN {
            return Err(MulanError::Corrupted(format!(
                "internal key too short: {} bytes",
                bytes.len()
            )));
        }
        let user_key = bytes[..bytes.len() - FOOTER_LEN].to_vec();
        let seq_bytes: [u8; SEQ_LEN] = bytes[bytes.len() - FOOTER_LEN..bytes.len() - TYPE_LEN]
            .try_into()
            .map_err(|_| MulanError::Corrupted("seq slice mismatch".into()))?;
        let vtype_byte = bytes[bytes.len() - TYPE_LEN];
        let vtype = ValueType::from_u8(vtype_byte).ok_or_else(|| {
            MulanError::Corrupted(format!("unknown value type byte: {vtype_byte}"))
        })?;
        Ok(InternalKey {
            user_key,
            seq: u64::from_le_bytes(seq_bytes),
            vtype,
        })
    }
}

impl PartialEq for InternalKey {
    fn eq(&self, other: &Self) -> bool {
        self.user_key == other.user_key && self.seq == other.seq && self.vtype == other.vtype
    }
}

impl Eq for InternalKey {}

/// internal key 字节比较器：按 InternalKey Ord 比较（user_key 字典序升序；同 user_key 下 seq 降序）。
///
/// 输入是 encode() 产生的字节（`user_key + seq 小端 + type`）。
/// 先比较 user_key 字节字典序；user_key 相同时，比较 seq（小端整数）的**降序**——
/// 大 seq 在前，让最新版本最先被查找命中。
///
/// 供 Block/SSTable 的查找用：Block 存 encode 字节，查找时用此闭包比较，
/// 避免纯字节字典序在变长 user_key 前缀关系下的错乱（见 docs/plan.md 约束 #4）。
pub fn internal_key_cmp(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let a_uk = user_key_of_internal_key(a);
    let b_uk = user_key_of_internal_key(b);
    match a_uk.cmp(b_uk) {
        std::cmp::Ordering::Equal => {}
        ord => return ord,
    }
    // 同 user_key：比 seq 降序。从 encode 末尾 9 字节解析 seq（小端）。
    let a_seq = seq_of_internal_key(a);
    let b_seq = seq_of_internal_key(b);
    a_seq.cmp(&b_seq).reverse()
}

/// 从 internal key 字节提取 user_key（去掉末尾 seq 小端 8 字节 + type 1 字节）。
pub fn user_key_of_internal_key(bytes: &[u8]) -> &[u8] {
    if bytes.len() < FOOTER_LEN {
        return bytes;
    }
    &bytes[..bytes.len() - FOOTER_LEN]
}

/// 从 internal key 字节提取 seq（小端）。无 footer 时返回 0。
fn seq_of_internal_key(bytes: &[u8]) -> u64 {
    if bytes.len() < FOOTER_LEN {
        return 0;
    }
    let start = bytes.len() - FOOTER_LEN;
    let seq_bytes: [u8; SEQ_LEN] = bytes[start..start + SEQ_LEN].try_into().unwrap();
    u64::from_le_bytes(seq_bytes)
}

/// 构造查询用的哨兵 internal key 字节：`user_key + MAX_SEQUENCE 小端 + Put`。
/// MAX_SEQUENCE 比所有真实 seq 大，故此 key 在同 user_key 下排最后（seq 降序下最大 seq 排最前，
/// 但 lower_bound 找第一个 >= 哨兵——同 user_key 的真实 seq 都 < MAX，所以真实版本 > 哨兵？）
///
/// 注意：internal_key_cmp 中 seq 是降序，所以"大 seq"在 Ord 上更小。
/// 哨兵 seq=MAX 是最大的，在同 user_key 下 Ord 最小（排最前）。
/// lower_bound(哨兵) 找第一个 Ord >= 哨兵的 → 同 user_key 的所有真实版本都 Ord > 哨兵（seq 更小 → Ord 更大），
/// 故命中的是 Ord 最小的真实版本 = seq 最大的最新版本 ✓。
pub fn lookup_key(user_key: &[u8]) -> Vec<u8> {
    InternalKey::new(user_key.to_vec(), MAX_SEQUENCE, ValueType::Put).encode()
}

impl Default for InternalKey {
    fn default() -> Self {
        // 跳表 head 的占位 key，永不参与比较。
        InternalKey::new(Vec::new(), 0, ValueType::Put)
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // 先按 user_key 字节字典序。
        match self.user_key.cmp(&other.user_key) {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        // 同 user_key 下 seq 降序：reverse 让大 seq 在前。
        match self.seq.cmp(&other.seq).reverse() {
            std::cmp::Ordering::Equal => {}
            ord => return ord,
        }
        // seq 相同时按 vtype（数值序）兜底，保证全序。
        (self.vtype as u8).cmp(&(other.vtype as u8))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        for (user_key, seq, vtype) in [
            (b"k1" as &[u8], 0u64, ValueType::Put),
            (b"hello", 42u64, ValueType::Delete),
            (b"", MAX_SEQUENCE, ValueType::Put),
            (&[0u8, 255, 7], 1, ValueType::Delete),
        ] {
            let ik = InternalKey::new(user_key.to_vec(), seq, vtype);
            let decoded = InternalKey::decode(&ik.encode()).unwrap();
            assert_eq!(ik, decoded);
        }
    }

    #[test]
    fn larger_seq_sorts_before_smaller() {
        // 同 user_key，大 seq 排前（cmp 返回 Greater 表示 self 在 other 之后）。
        let high = InternalKey::new(b"key".to_vec(), 100, ValueType::Put);
        let low = InternalKey::new(b"key".to_vec(), 5, ValueType::Put);
        assert!(high < low, "seq=100 should sort before seq=5");
    }

    #[test]
    fn cross_byte_boundary_seq_255_256() {
        // 关键跨边界测试：seq=255 和 256 在小端字节下低字节从 0xFF 跳到 0x00，
        // 这是原取反方案崩溃的边界。Ord 方案必须正确处理。
        let s255 = InternalKey::new(b"k".to_vec(), 255, ValueType::Put);
        let s256 = InternalKey::new(b"k".to_vec(), 256, ValueType::Put);
        assert!(s256 < s255, "seq=256 should sort before seq=255");
        // 再验证 encode 后不参与排序：即便字节布局"乱"，Ord 仍正确。
        assert!(
            InternalKey::decode(&s256.encode()).unwrap()
                < InternalKey::decode(&s255.encode()).unwrap()
        );
    }

    #[test]
    fn seq_zero_vs_max() {
        let zero = InternalKey::new(b"k".to_vec(), 0, ValueType::Put);
        let max = InternalKey::new(b"k".to_vec(), MAX_SEQUENCE, ValueType::Put);
        assert!(max < zero, "MAX_SEQUENCE should sort before 0");
    }

    #[test]
    fn different_user_keys_in_lexicographic_order() {
        let a = InternalKey::new(b"apple".to_vec(), 9, ValueType::Put);
        let b = InternalKey::new(b"banana".to_vec(), 1, ValueType::Put);
        assert!(a < b, "apple should sort before banana");
    }

    #[test]
    fn decode_rejects_too_short_input() {
        assert!(InternalKey::decode(b"").is_err());
        assert!(InternalKey::decode(b"short").is_err());
        assert!(InternalKey::decode(&[0u8; 8]).is_err());
    }

    #[test]
    fn decode_rejects_unknown_value_type() {
        let mut bytes = InternalKey::new(b"k".to_vec(), 1, ValueType::Put).encode();
        *bytes.last_mut().unwrap() = 99;
        assert!(InternalKey::decode(&bytes).is_err());
    }

    #[test]
    fn delete_encodes_as_type_one() {
        let bytes = InternalKey::new(b"k".to_vec(), 1, ValueType::Delete).encode();
        assert_eq!(*bytes.last().unwrap(), 1);
    }

    /// 核心不变量：InternalKey Ord 在各种 key 关系下的正确性。
    /// 这是 SSTable 排序的合法性基础（Block 查找用 internal_key_cmp 按 Ord 比较）。
    #[test]
    fn internal_key_ord_correctness() {
        let cases: Vec<(InternalKey, InternalKey, &str)> = vec![
            (
                InternalKey::new(b"apple".to_vec(), 5, ValueType::Put),
                InternalKey::new(b"banana".to_vec(), 1, ValueType::Put),
                "apple < banana",
            ),
            (
                InternalKey::new(b"k".to_vec(), 100, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 5, ValueType::Put),
                "seq=100 < seq=5 (降序)",
            ),
            (
                InternalKey::new(b"k".to_vec(), 256, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 255, ValueType::Put),
                "seq=256 < seq=255 跨字节边界",
            ),
            (
                InternalKey::new(b"k".to_vec(), MAX_SEQUENCE, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 0, ValueType::Put),
                "seq=MAX < seq=0",
            ),
            (
                InternalKey::new(b"present-2".to_vec(), 1, ValueType::Put),
                InternalKey::new(b"present-200".to_vec(), 1, ValueType::Put),
                "前缀: present-2 < present-200",
            ),
            (
                InternalKey::new(b"a".to_vec(), 1, ValueType::Put),
                InternalKey::new(b"a\xFF".to_vec(), 1, ValueType::Put),
                "前缀: 'a' < 'a'+0xFF",
            ),
        ];
        for (a, b, desc) in cases {
            assert!(a < b, "{desc}: expected a < b but got {:?}", a.cmp(&b));
        }
    }
}
