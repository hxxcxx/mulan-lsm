//! 变长整数编码（varint）：用每个字节的最高位作为"是否还有后续字节"的标志，
//! 低 7 位承载有效数据。小数值用 1 字节，大数值最多 5（u32）/10（u64）字节。
//! LevelDB 和 protobuf 通用此编码，SSTable/MemTable/WAL 都用它压缩长度字段。

use crate::error::{MulanError, Result};

const MORE_BIT: u8 = 0x80;
const PAYLOAD_MASK: u8 = 0x7f;
/// varint32 最多 5 字节（每字节 7 位，⌈32/7⌉=5）。
const MAX_VARINT32_LEN: usize = 5;
/// varint64 最多 10 字节（⌈64/7⌉=10）。
const MAX_VARINT64_LEN: usize = 10;

/// 编码 u32 为 varint，追加到 buf。返回写入字节数。
pub fn encode_varint32(buf: &mut Vec<u8>, mut value: u32) -> usize {
    let start = buf.len();
    while value >= 0x80 {
        buf.push((value as u8) | MORE_BIT);
        value >>= 7;
    }
    buf.push(value as u8);
    buf.len() - start
}

/// 编码 u64 为 varint，追加到 buf。返回写入字节数。
pub fn encode_varint64(buf: &mut Vec<u8>, mut value: u64) -> usize {
    let start = buf.len();
    while value >= 0x80 {
        buf.push((value as u8) | MORE_BIT);
        value >>= 7;
    }
    buf.push(value as u8);
    buf.len() - start
}

/// 从 buf 的指定位置解码 varint32。返回 (值, 消耗字节数)。
pub fn decode_varint32(buf: &[u8]) -> Result<(u32, usize)> {
    decode_varint(buf, MAX_VARINT32_LEN).map(|(v, n)| (v as u32, n))
}

/// 从 buf 的指定位置解码 varint64。返回 (值, 消耗字节数)。
pub fn decode_varint64(buf: &[u8]) -> Result<(u64, usize)> {
    decode_varint(buf, MAX_VARINT64_LEN)
}

/// varint 解码核心。每取一字节，低 7 位拼进结果，最高位为 0 表示结束。
fn decode_varint(buf: &[u8], max_len: usize) -> Result<(u64, usize)> {
    let mut result: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in buf.iter().enumerate().take(max_len) {
        result |= ((byte & PAYLOAD_MASK) as u64) << shift;
        if byte & MORE_BIT == 0 {
            return Ok((result, i + 1));
        }
        shift += 7;
    }
    Err(MulanError::Corrupted(format!(
        "varint too long or unterminated (max {max_len} bytes)"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip32(value: u32) {
        let mut buf = Vec::new();
        encode_varint32(&mut buf, value);
        let (decoded, n) = decode_varint32(&buf).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(n, buf.len());
    }

    fn roundtrip64(value: u64) {
        let mut buf = Vec::new();
        encode_varint64(&mut buf, value);
        let (decoded, n) = decode_varint64(&buf).unwrap();
        assert_eq!(decoded, value);
        assert_eq!(n, buf.len());
    }

    #[test]
    fn varint32_small_values() {
        // < 128 的值只需 1 字节。
        for v in [0u32, 1, 127] {
            let mut buf = Vec::new();
            let n = encode_varint32(&mut buf, v);
            assert_eq!(n, 1);
            roundtrip32(v);
        }
    }

    #[test]
    fn varint32_boundary_values() {
        // 跨字节边界的关键值。
        for v in [128u32, 129, 255, 256, 16383, 16384, 16385, u32::MAX] {
            roundtrip32(v);
        }
    }

    #[test]
    fn varint64_large_values() {
        for v in [0u64, 127, 128, u32::MAX as u64, u64::MAX - 1, u64::MAX] {
            roundtrip64(v);
        }
    }

    #[test]
    fn varint_multiple_in_buffer() {
        // 多个 varint 连续放在一个 buffer 里，逐个解码。
        let mut buf = Vec::new();
        let values = [300u32, 1, 50000, 127, 128];
        for &v in &values {
            encode_varint32(&mut buf, v);
        }
        let mut pos = 0;
        for &expected in &values {
            let (v, n) = decode_varint32(&buf[pos..]).unwrap();
            assert_eq!(v, expected);
            pos += n;
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn varint_rejects_truncated() {
        // 截断的 varint（最高位一直为 1，没有终止字节）。
        let buf = [0x80u8, 0x80, 0x80];
        assert!(decode_varint32(&buf).is_err());
    }

    #[test]
    fn varint_rejects_empty() {
        assert!(decode_varint32(&[]).is_err());
    }

    #[test]
    fn var32_max_uses_five_bytes() {
        let mut buf = Vec::new();
        let n = encode_varint32(&mut buf, u32::MAX);
        // u32::MAX = 0xFFFFFFFF，需要 ⌈32/7⌉ = 5 字节。
        assert_eq!(n, 5);
        assert_eq!(decode_varint32(&buf).unwrap().0, u32::MAX);
    }

    #[test]
    fn var64_max_uses_ten_bytes() {
        let mut buf = Vec::new();
        let n = encode_varint64(&mut buf, u64::MAX);
        assert_eq!(n, 10);
        assert_eq!(decode_varint64(&buf).unwrap().0, u64::MAX);
    }
}
