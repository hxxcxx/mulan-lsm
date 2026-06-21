//! InternalKey：把 user_key 扩展成 `user_key + !seq + type`，用一段可比较字节
//! 同时承载"多版本"和"删除"。

use crate::error::{MulanError, Result};

/// InternalKey 末尾固定 9 字节：8 字节 sequence + 1 字节 type。
const FOOTER_LEN: usize = 9;
const SEQ_LEN: usize = 8;
const TYPE_LEN: usize = 1;

/// 序列号最大值。原版保留 u64::MAX 作为查找哨兵，实际使用的 seq 上限比它小 1。
pub const MAX_SEQUENCE: u64 = u64::MAX - 1;

/// 写入操作的类型。Delete 不是真删除，而是写入一个删除标记。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Put = 0,
    Delete = 1,
}

impl ValueType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(ValueType::Put),
            1 => Some(ValueType::Delete),
            _ => None,
        }
    }
}

/// 解析后的 InternalKey，零拷贝引用原字节。
#[derive(Debug, PartialEq, Eq)]
pub struct ParsedInternalKey<'a> {
    pub user_key: &'a [u8],
    pub seq: u64,
    pub vtype: ValueType,
}

/// 构造 InternalKey 字节：`user_key + (!seq 小端) + type`。
///
/// seq 按位取反存储，是这套编码的核心技巧：
/// `a > b` 当且仅当 `!a < !b`，所以"大 seq"取反后在字节序上变小，天然排在前。
/// 这样同一 user_key 的新版本自动排前，查找时第一个命中即最新，
/// 且整个 key 可直接用字节字典序比较，无需自定义比较器。
pub fn make(user_key: &[u8], seq: u64, vtype: ValueType) -> Vec<u8> {
    let mut buf = Vec::with_capacity(user_key.len() + FOOTER_LEN);
    buf.extend_from_slice(user_key);
    buf.extend_from_slice(&(!seq).to_le_bytes());
    buf.push(vtype as u8);
    buf
}

/// 返回 InternalKey 中的 user_key 部分（截掉末尾 9 字节）。
/// 长度不足时返回 Corrupted 错误。
pub fn user_key(internal_key: &[u8]) -> Result<&[u8]> {
    if internal_key.len() < FOOTER_LEN {
        return Err(MulanError::Corrupted(format!(
            "internal key too short: {} bytes",
            internal_key.len()
        )));
    }
    Ok(&internal_key[..internal_key.len() - FOOTER_LEN])
}

/// 解析 InternalKey 的全部字段。user_key 零拷贝引用输入字节。
pub fn parse(internal_key: &[u8]) -> Result<ParsedInternalKey<'_>> {
    if internal_key.len() < FOOTER_LEN {
        return Err(MulanError::Corrupted(format!(
            "internal key too short: {} bytes",
            internal_key.len()
        )));
    }
    let user_key = &internal_key[..internal_key.len() - FOOTER_LEN];
    let seq_bytes: [u8; SEQ_LEN] = internal_key
        [internal_key.len() - FOOTER_LEN..internal_key.len() - TYPE_LEN]
        .try_into()
        .map_err(|_| MulanError::Corrupted("seq slice mismatch".into()))?;
    let vtype_byte = internal_key[internal_key.len() - TYPE_LEN];
    let vtype = ValueType::from_u8(vtype_byte)
        .ok_or_else(|| MulanError::Corrupted(format!("unknown value type byte: {vtype_byte}")))?;
    // 构造时写入的是 !seq 的小端字节；这里逆操作：读出后按位取反还原 seq。
    let seq_inverted = u64::from_le_bytes(seq_bytes);
    let seq = !seq_inverted;
    Ok(ParsedInternalKey {
        user_key,
        seq,
        vtype,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_parse_round_trip() {
        for (user_key, seq, vtype) in [
            (b"k1" as &[u8], 0u64, ValueType::Put),
            (b"hello", 42u64, ValueType::Delete),
            (b"", MAX_SEQUENCE, ValueType::Put),
            (&[0u8, 255, 7], 1, ValueType::Delete),
        ] {
            let bytes = make(user_key, seq, vtype);
            let parsed = parse(&bytes).unwrap();
            assert_eq!(parsed.user_key, user_key);
            assert_eq!(parsed.seq, seq);
            assert_eq!(parsed.vtype, vtype);
        }
    }

    #[test]
    fn larger_seq_sorts_before_smaller() {
        // 同一 user_key，大 seq 的 internal key 在字节序上更小（排前面）。
        let k_high = make(b"key", 100, ValueType::Put);
        let k_low = make(b"key", 5, ValueType::Put);
        assert!(k_high < k_low, "seq=100 should sort before seq=5");
    }

    #[test]
    fn different_user_keys_in_lexicographic_order() {
        let a = make(b"apple", 9, ValueType::Put);
        let b = make(b"banana", 1, ValueType::Put);
        assert!(a < b, "apple should sort before banana");
    }

    #[test]
    fn user_key_extraction_ignores_footer() {
        let bytes = make(b"mykey", 7, ValueType::Delete);
        assert_eq!(user_key(&bytes).unwrap(), b"mykey");
    }

    #[test]
    fn parse_rejects_too_short_input() {
        assert!(parse(b"").is_err());
        assert!(parse(b"short").is_err());
        assert!(parse(&[0u8; 8]).is_err());
    }

    #[test]
    fn parse_rejects_unknown_value_type() {
        let mut bytes = make(b"k", 1, ValueType::Put);
        // 篡改 type 字节为非法值。
        *bytes.last_mut().unwrap() = 99;
        assert!(parse(&bytes).is_err());
    }

    #[test]
    fn delete_encodes_as_type_one() {
        let bytes = make(b"k", 1, ValueType::Delete);
        assert_eq!(*bytes.last().unwrap(), 1);
    }
}
