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

    /// 排序键编码：`user_key + (!seq 大端 8 字节) + type`。
    ///
    /// 专供 Block/SSTable 用——保证 sort_key 的字节字典序 == InternalKey 的 Ord：
    /// - user_key 字典序（Ord 第一关键字）
    /// - !seq 大端：大端下字节序 == 整数序，取反让"大 seq → 小字节序"，即同 user_key 下大 seq 在前（Ord 第二关键字降序）
    /// - type 占位（seq 唯一时不影响排序）
    ///
    /// 与 encode()（小端，WAL 用）职责分离，不可混用。详见 docs/plan.md 设计约束 #1。
    pub fn sort_key(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.user_key.len() + FOOTER_LEN);
        buf.extend_from_slice(&self.user_key);
        buf.extend_from_slice(&(!self.seq).to_be_bytes());
        buf.push(self.vtype as u8);
        buf
    }

    /// 从 sort_key 还原 InternalKey。
    pub fn from_sort_key(sort_key: &[u8]) -> Result<Self> {
        if sort_key.len() < FOOTER_LEN {
            return Err(MulanError::Corrupted(format!(
                "sort key too short: {} bytes",
                sort_key.len()
            )));
        }
        let user_key = sort_key[..sort_key.len() - FOOTER_LEN].to_vec();
        let seq_bytes: [u8; SEQ_LEN] = sort_key
            [sort_key.len() - FOOTER_LEN..sort_key.len() - TYPE_LEN]
            .try_into()
            .map_err(|_| MulanError::Corrupted("sort key seq slice mismatch".into()))?;
        let vtype_byte = sort_key[sort_key.len() - TYPE_LEN];
        let vtype = ValueType::from_u8(vtype_byte).ok_or_else(|| {
            MulanError::Corrupted(format!("unknown value type byte in sort key: {vtype_byte}"))
        })?;
        Ok(InternalKey {
            user_key,
            // 写入时是 !seq 大端，读出大端值后取反还原。
            seq: !u64::from_be_bytes(seq_bytes),
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

    /// 核心不变量：sort_key 字节字典序 == InternalKey Ord。
    /// 这是 SSTable/Block 用字节比较的合法性基础。违反则 SSTable 查找全错。
    #[test]
    fn sort_key_byte_order_matches_ord() {
        // 构造一组覆盖各种排序关系的 key 对。
        let cases: Vec<(InternalKey, InternalKey, &str)> = vec![
            // 不同 user_key，字典序。
            (
                InternalKey::new(b"apple".to_vec(), 5, ValueType::Put),
                InternalKey::new(b"banana".to_vec(), 1, ValueType::Put),
                "apple < banana",
            ),
            // 同 user_key，大 seq 在前。
            (
                InternalKey::new(b"k".to_vec(), 100, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 5, ValueType::Put),
                "seq=100 < seq=5",
            ),
            // 跨字节边界：seq=256（大端低字节变化处）。
            (
                InternalKey::new(b"k".to_vec(), 256, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 255, ValueType::Put),
                "seq=256 < seq=255",
            ),
            // 极端：seq=0 vs MAX。
            (
                InternalKey::new(b"k".to_vec(), MAX_SEQUENCE, ValueType::Put),
                InternalKey::new(b"k".to_vec(), 0, ValueType::Put),
                "seq=MAX < seq=0",
            ),
        ];
        for (a, b, desc) in cases {
            let ord = a.cmp(&b);
            let byte_ord = a.sort_key().cmp(&b.sort_key());
            assert_eq!(
                ord, byte_ord,
                "{desc}: Ord={ord:?} but sort_key byte order={byte_ord:?}"
            );
        }
    }

    #[test]
    fn sort_key_round_trip() {
        for (user_key, seq, vtype) in [
            (b"k1" as &[u8], 0u64, ValueType::Put),
            (b"k", 255u64, ValueType::Delete),
            (b"long-user-key-123", 256u64, ValueType::Put),
            (b"", MAX_SEQUENCE, ValueType::Put),
        ] {
            let ik = InternalKey::new(user_key.to_vec(), seq, vtype);
            let restored = InternalKey::from_sort_key(&ik.sort_key()).unwrap();
            assert_eq!(ik, restored);
        }
    }
}
