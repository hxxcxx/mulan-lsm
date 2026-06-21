//! WAL（Write-Ahead Log）预写日志：MemTable 写入先顺序追加到此，崩溃后回放重建。
//!
//! 文件布局：一串固定大小的 block。每个 block 含若干 record。
//! record 格式（7 字节头 + 数据）：checksum(4) | length(2) | type(1) | data[length]
//!
//! block 末尾不足 7 字节（放不下一个 record 头）时填 0 跳过（trailer）。

use std::io::Write;

/// block 固定大小 32KB。损坏时跳到下一个 block 边界，把损坏限制在单 block 内。
pub const BLOCK_SIZE: usize = 32_768;

/// record 头：4(crc32c) + 2(length) + 1(type)。
pub const HEADER_SIZE: usize = 7;

/// 单个 record 可承载的最大数据长度。
pub const DATA_MAX: usize = BLOCK_SIZE - HEADER_SIZE;

/// record 分片类型。
/// 第一版只实现 FULL（单条数据 < DATA_MAX，不分片）。
/// FIRST/MIDDLE/LAST 用于大 record 跨 block 分片，留作后续进阶。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RecordType {
    /// 整条记录装在一个 record 中，未分片。
    Full = 1,
    /// 大记录的第一片。
    First = 2,
    /// 大记录的中间片。
    Middle = 3,
    /// 大记录的最后一片。
    Last = 4,
}

impl RecordType {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            1 => Some(RecordType::Full),
            2 => Some(RecordType::First),
            3 => Some(RecordType::Middle),
            4 => Some(RecordType::Last),
            _ => None,
        }
    }
}

/// record 头解析结果。
#[derive(Debug, PartialEq, Eq)]
pub struct RecordHeader {
    pub checksum: u32,
    pub length: u16,
    pub rtype: RecordType,
}

/// 把一条 FULL record 编码进 block 缓冲区的当前位置。
///
/// 调用方需保证 buf 有足够容量（HEADER_SIZE + data.len()），且 data.len() <= DATA_MAX。
/// 返回写入的字节数（HEADER_SIZE + data.len()）。
pub fn encode_full(buf: &mut Vec<u8>, data: &[u8]) {
    debug_assert!(data.len() <= DATA_MAX);
    // crc32c 覆盖 type + data，这样校验同时防头部 type 篡改和数据损坏。
    let mut crc_input = Vec::with_capacity(1 + data.len());
    crc_input.push(RecordType::Full as u8);
    crc_input.extend_from_slice(data);
    let checksum = crc32c(&crc_input);

    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u16).to_le_bytes());
    buf.push(RecordType::Full as u8);
    buf.extend_from_slice(data);
}

/// 解析 7 字节头。返回头和 data 在 buf 中的起始偏移（HEADER_SIZE 处）。
/// 头部字段非法（type 未知）时返回错误。
pub fn decode_header(buf: &[u8; HEADER_SIZE]) -> crate::error::Result<RecordHeader> {
    let checksum = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let length = u16::from_le_bytes([buf[4], buf[5]]);
    let rtype = RecordType::from_u8(buf[6]).ok_or_else(|| {
        crate::error::MulanError::Corrupted(format!("unknown record type: {}", buf[6]))
    })?;
    Ok(RecordHeader {
        checksum,
        length,
        rtype,
    })
}

/// 校验一条 record 的 crc32c 是否匹配（type + data）。
pub fn verify_checksum(rtype: RecordType, data: &[u8], expected: u32) -> bool {
    let mut crc_input = Vec::with_capacity(1 + data.len());
    crc_input.push(rtype as u8);
    crc_input.extend_from_slice(data);
    crc32c(&crc_input) == expected
}

/// crc32c（Castagnoli 多项式）。LevelDB 用此变体，非普通 crc32。
fn crc32c(data: &[u8]) -> u32 {
    use crc::Crc;
    const CRC32C: Crc<u32> = Crc::<u32>::new(&crc::CRC_32_ISCSI);
    CRC32C.checksum(data)
}

/// 把一个分片（任意 type）编码进 buf。crc32c 覆盖 type + data。
fn encode_piece(buf: &mut Vec<u8>, rtype: RecordType, data: &[u8]) {
    let mut crc_input = Vec::with_capacity(1 + data.len());
    crc_input.push(rtype as u8);
    crc_input.extend_from_slice(data);
    let checksum = crc32c(&crc_input);

    buf.extend_from_slice(&checksum.to_le_bytes());
    buf.extend_from_slice(&(data.len() as u16).to_le_bytes());
    buf.push(rtype as u8);
    buf.extend_from_slice(data);
}

/// WAL 写入器。把任意大小的 record 顺序追加到文件。
/// 超过单个 block 容量的 record 自动切成 FIRST/MIDDLE/LAST 分片。
pub struct WalWriter {
    file: std::fs::File,
    /// 当前 block 已写入的字节数（0..BLOCK_SIZE）。
    block_offset: usize,
}

impl WalWriter {
    /// 以追加模式打开（不存在则创建）。从文件尾继续写。
    /// block_offset 取 文件大小 % BLOCK_SIZE；若文件不是整数个 block，
    /// 视为从某个 block 中间继续，由分片逻辑自然处理。
    pub fn create(path: &std::path::Path) -> crate::error::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let len = file.metadata()?.len() as usize;
        Ok(WalWriter {
            file,
            block_offset: len % BLOCK_SIZE,
        })
    }

    /// 追加一条记录。任意大小都会被正确分片。
    pub fn add_record(&mut self, data: &[u8]) -> crate::error::Result<()> {
        let mut remaining = data;
        let mut first = true;
        loop {
            let leftover = BLOCK_SIZE - block_offset_safe(self.block_offset);
            if leftover < HEADER_SIZE {
                // 当前 block 剩余放不下一个头，填 0 trailer 到 block 末尾。
                if leftover > 0 {
                    self.file.write_all(&vec![0u8; leftover])?;
                }
                self.block_offset = 0;
            }
            // 这一片最多能放多少数据。
            let avail = BLOCK_SIZE - HEADER_SIZE - self.block_offset;
            let piece_len = remaining.len().min(avail);
            let piece = &remaining[..piece_len];
            let last = piece_len == remaining.len();

            let rtype = match (first, last) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            let mut buf = Vec::with_capacity(HEADER_SIZE + piece_len);
            encode_piece(&mut buf, rtype, piece);
            self.file.write_all(&buf)?;
            self.block_offset += HEADER_SIZE + piece_len;

            remaining = &remaining[piece_len..];
            first = false;
            if remaining.is_empty() {
                break;
            }
        }
        Ok(())
    }

    /// 把缓冲的数据刷到磁盘。默认 add_record 不自动 fsync，由上层按需调用。
    pub fn sync(&mut self) -> crate::error::Result<()> {
        self.file.sync_all()?;
        Ok(())
    }

    /// 关闭前刷盘。
    pub fn flush(&mut self) -> crate::error::Result<()> {
        self.file.flush()?;
        Ok(())
    }
}

/// 计算剩余空间，防御 block_offset 超出 BLOCK_SIZE 的异常情况。
fn block_offset_safe(off: usize) -> usize {
    if off > BLOCK_SIZE {
        BLOCK_SIZE
    } else {
        off
    }
}

/// WAL 读取器。扫描整个文件，拼装 FIRST/MIDDLE/LAST 分片回完整 record。
///
/// 崩溃语义：遇到 crc 校验失败的片（写了一半的残片）立即停止，
/// 返回此前已完整拼装的所有 record。这是"最近写入可能丢失"的安全保证——
/// 无需额外的完成标记，靠 crc 自动识别残片。
pub struct WalReader {
    bytes: Vec<u8>,
}

impl WalReader {
    /// 把整个文件读进内存后扫描。WAL 通常不大，简化实现。
    pub fn open(path: &std::path::Path) -> crate::error::Result<Self> {
        let bytes = std::fs::read(path)?;
        Ok(WalReader { bytes })
    }

    /// 扫描所有 block，按状态机拼装 record，返回完整记录的数据列表。
    /// 遇到 crc 失败的片则停止，丢弃正在拼装的残缺记录。
    pub fn read_records(&self) -> crate::error::Result<Vec<Vec<u8>>> {
        let mut records: Vec<Vec<u8>> = Vec::new();
        let mut fragment: Option<Vec<u8>> = None; // 正在拼装的记录缓存
        let mut pos = 0;

        while pos + HEADER_SIZE <= self.bytes.len() {
            let block_start = (pos / BLOCK_SIZE) * BLOCK_SIZE;
            // 当前 block 内剩余字节。
            let block_remaining = BLOCK_SIZE - (pos - block_start);
            // 剩余放不下一个头 → 跳到下一 block（处理 trailer 填充区）。
            if block_remaining < HEADER_SIZE {
                pos = block_start + BLOCK_SIZE;
                continue;
            }

            let header_bytes: &[u8; HEADER_SIZE] =
                self.bytes[pos..pos + HEADER_SIZE].try_into().unwrap();
            // type 字节为 0 是 trailer 填充（length=0, type=0 非法），跳到下一 block。
            if header_bytes[6] == 0 {
                pos = block_start + BLOCK_SIZE;
                continue;
            }

            let header = match decode_header(header_bytes) {
                Ok(h) => h,
                Err(_) => {
                    // 头部损坏，按崩溃残留处理，停止。
                    break;
                }
            };
            let data_start = pos + HEADER_SIZE;
            let data_end = data_start + header.length as usize;
            if data_end > self.bytes.len() {
                // 数据超出文件尾，写了一半的残片，停止。
                break;
            }
            let data = &self.bytes[data_start..data_end];

            // crc 校验。失败说明是写一半的残片，停止。
            if !verify_checksum(header.rtype, data, header.checksum) {
                break;
            }

            // 状态机：根据当前是否在拼装 + 这片 type 决定动作。
            let in_fragment = fragment.is_some();
            match (in_fragment, header.rtype) {
                (false, RecordType::Full) => {
                    records.push(data.to_vec());
                }
                (false, RecordType::First) => {
                    fragment = Some(data.to_vec());
                }
                (false, RecordType::Middle | RecordType::Last) => {
                    // idle 时遇 Middle/Last：上条残缺或损坏，丢弃，继续扫。
                }
                (true, RecordType::Middle) => {
                    fragment.as_mut().unwrap().extend_from_slice(data);
                }
                (true, RecordType::Last) => {
                    fragment.as_mut().unwrap().extend_from_slice(data);
                    records.push(fragment.take().unwrap());
                }
                (true, RecordType::Full | RecordType::First) => {
                    // 拼装中遇 Full/First：上条残缺，丢弃缓存，把新片作为起点。
                    if header.rtype == RecordType::Full {
                        records.push(data.to_vec());
                        fragment = None;
                    } else {
                        fragment = Some(data.to_vec());
                    }
                }
            }

            pos = data_end;
        }

        // 文件正常结束但仍有未完成的 fragment：视作残缺丢弃。
        Ok(records)
    }
}

/// 解析后的 WAL entry：一条用户写操作（put 或 delete）。
#[derive(Debug, PartialEq, Eq)]
pub struct WalEntry {
    pub vtype: crate::internal_key::ValueType,
    pub seq: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// 把一条写操作编码为 record 的 data 部分。
///
/// 布局：vtype(1) | seq(8, 小端) | key_len(varint) | key | val_len(varint) | val
/// seq 直接落地（不取反），因为 WAL 不参与排序，只是顺序回放。
/// Delete 的 value 编码为空（val_len=0）。
pub fn encode_entry(
    vtype: crate::internal_key::ValueType,
    seq: u64,
    key: &[u8],
    value: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9 + key.len() + value.len());
    buf.push(vtype as u8);
    buf.extend_from_slice(&seq.to_le_bytes());
    crate::varint::encode_varint32(&mut buf, key.len() as u32);
    buf.extend_from_slice(key);
    crate::varint::encode_varint32(&mut buf, value.len() as u32);
    buf.extend_from_slice(value);
    buf
}

/// 解码一条 entry。返回拥有的 WalEntry。
pub fn decode_entry(data: &[u8]) -> crate::error::Result<WalEntry> {
    use crate::error::MulanError;
    use crate::internal_key::ValueType;
    if data.len() < 9 {
        return Err(MulanError::Corrupted(format!(
            "entry too short: {} bytes",
            data.len()
        )));
    }
    let vtype = ValueType::from_u8(data[0])
        .ok_or_else(|| MulanError::Corrupted(format!("unknown value type: {}", data[0])))?;
    let seq = u64::from_le_bytes(data[1..9].try_into().unwrap());
    let (key_len, consumed_kl) = crate::varint::decode_varint32(&data[9..])?;
    let key_end = 9 + consumed_kl + key_len as usize;
    if key_end > data.len() {
        return Err(MulanError::Corrupted("entry key out of bounds".into()));
    }
    let key = data[9 + consumed_kl..key_end].to_vec();
    let (val_len, consumed_vl) = crate::varint::decode_varint32(&data[key_end..])?;
    let val_start = key_end + consumed_vl;
    let val_end = val_start + val_len as usize;
    if val_end > data.len() {
        return Err(MulanError::Corrupted("entry value out of bounds".into()));
    }
    let value = data[val_start..val_end].to_vec();
    Ok(WalEntry {
        vtype,
        seq,
        key,
        value,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

    // 每个测试一个独立临时目录，避免并发测试相互干扰。零外部依赖。
    fn tempfile_dir() -> std::path::PathBuf {
        let n = DIR_COUNTER.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!("mulan-wal-test-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn encode_full_writes_header_then_data() {
        let mut buf = Vec::new();
        let data = b"hello world";
        encode_full(&mut buf, data);
        // 7 字节头 + 11 字节数据。
        assert_eq!(buf.len(), HEADER_SIZE + data.len());

        // 解析头。
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let header = decode_header(&header_bytes).unwrap();
        assert_eq!(header.rtype, RecordType::Full);
        assert_eq!(header.length as usize, data.len());
        // crc 校验通过。
        assert!(verify_checksum(
            header.rtype,
            &buf[HEADER_SIZE..],
            header.checksum
        ));
        // 数据部分原样。
        assert_eq!(&buf[HEADER_SIZE..], data);
    }

    #[test]
    fn encode_full_empty_data() {
        // 空 data 也合法（删除标记的 value 为空时会出现）。
        let mut buf = Vec::new();
        encode_full(&mut buf, b"");
        assert_eq!(buf.len(), HEADER_SIZE);
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let header = decode_header(&header_bytes).unwrap();
        assert_eq!(header.length, 0);
        assert!(verify_checksum(header.rtype, &[], header.checksum));
    }

    #[test]
    fn corrupted_data_fails_checksum() {
        let mut buf = Vec::new();
        encode_full(&mut buf, b"original");
        // 篡改数据区的第一个字节。
        buf[HEADER_SIZE] ^= 0xFF;
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        let header = decode_header(&header_bytes).unwrap();
        assert!(!verify_checksum(
            header.rtype,
            &buf[HEADER_SIZE..],
            header.checksum
        ));
    }

    #[test]
    fn corrupted_type_fails_checksum() {
        // crc 覆盖 type，篡改 type 也会被检出。
        let mut buf = Vec::new();
        encode_full(&mut buf, b"abc");
        // 篡改 type 字节（位置 6）。
        buf[6] = 9;
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&buf[..HEADER_SIZE]);
        // type 非法，decode 直接报错。
        assert!(decode_header(&header_bytes).is_err());
    }

    #[test]
    fn data_max_consistent() {
        assert_eq!(DATA_MAX, BLOCK_SIZE - HEADER_SIZE);
        // 单条 record 上限略小于 block。
        assert_eq!(DATA_MAX, 32_761);
    }

    fn read_file(path: &std::path::Path) -> Vec<u8> {
        std::fs::read(path).unwrap()
    }

    /// 扫描文件，按 record 头解析，返回每条 record 的 (type, data)。
    /// 仅供测试验证字节布局，不拼装分片（拼装在 2.2 的 reader 做）。
    fn scan_records(bytes: &[u8]) -> Vec<(RecordType, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pos = 0;
        while pos < bytes.len() {
            let block_start = (pos / BLOCK_SIZE) * BLOCK_SIZE;
            let block_end = (block_start + BLOCK_SIZE).min(bytes.len());
            // block 内剩余。
            if block_end - pos < HEADER_SIZE {
                // 剩余不足一个头，跳到下一 block。
                pos = block_end;
                continue;
            }
            let mut header_bytes = [0u8; HEADER_SIZE];
            header_bytes.copy_from_slice(&bytes[pos..pos + HEADER_SIZE]);
            // trailer 填充区头部是全 0（length=0,type=0 非法），作为块尾标记跳过。
            let header = match decode_header(&header_bytes) {
                Ok(h) => h,
                Err(_) => {
                    // 非法 type（trailer 的 0），跳到下一 block。
                    pos = block_end;
                    continue;
                }
            };
            let data_start = pos + HEADER_SIZE;
            let data_end = data_start + header.length as usize;
            out.push((header.rtype, bytes[data_start..data_end].to_vec()));
            pos = data_end;
        }
        out
    }

    #[test]
    fn writer_small_record_is_full() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.add_record(b"hello").unwrap();
        w.flush().unwrap();
        drop(w);

        let bytes = read_file(&path);
        let records = scan_records(&bytes);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, RecordType::Full);
        assert_eq!(records[0].1, b"hello");
    }

    #[test]
    fn writer_multiple_small_records() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        w.add_record(b"a").unwrap();
        w.add_record(b"bb").unwrap();
        w.add_record(b"ccc").unwrap();
        w.flush().unwrap();
        drop(w);

        let records = scan_records(&read_file(&path));
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].1, b"a");
        assert_eq!(records[1].1, b"bb");
        assert_eq!(records[2].1, b"ccc");
        // 都是 FULL（小记录不分片）。
        for (rt, _) in &records {
            assert_eq!(*rt, RecordType::Full);
        }
    }

    #[test]
    fn writer_oversized_record_is_split() {
        // 超过单个 block 容量，必然分片。
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        let big = vec![0xABu8; BLOCK_SIZE + 10_000];
        w.add_record(&big).unwrap();
        w.flush().unwrap();
        drop(w);

        let records = scan_records(&read_file(&path));
        // 第一片 FIRST，后续若干 MIDDLE，最后 LAST。
        assert_eq!(records[0].0, RecordType::First);
        assert_eq!(records.last().unwrap().0, RecordType::Last);
        for r in &records[1..records.len() - 1] {
            assert_eq!(r.0, RecordType::Middle);
        }
        // 拼接所有分片数据，应等于原始 big。
        let reassembled: Vec<u8> = records
            .iter()
            .flat_map(|(_, d)| d.iter().copied())
            .collect();
        assert_eq!(reassembled, big);
    }

    #[test]
    fn writer_exact_one_block_record_is_full() {
        // 恰好等于 DATA_MAX 的记录，正好填满一个 block 的可用空间，是 FULL。
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        let exact = vec![0x11u8; DATA_MAX];
        w.add_record(&exact).unwrap();
        w.flush().unwrap();
        drop(w);

        let records = scan_records(&read_file(&path));
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, RecordType::Full);
        assert_eq!(records[0].1.len(), DATA_MAX);
        // 文件恰好一个 block。
        assert_eq!(read_file(&path).len(), BLOCK_SIZE);
    }

    #[test]
    fn writer_cross_block_with_trailer() {
        // 先写一条接近填满 block 的记录，再写一条会触发跨 block 的记录，
        // 验证 trailer 填充正确。
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        // 第一条：DATA_MAX - 5 字节，加上 7 字节头占 32763，留下 5 字节空间（不够下一个头）。
        let first = vec![0x01u8; DATA_MAX - 5];
        w.add_record(&first).unwrap();
        // 第二条：block 剩余 5 字节 < HEADER_SIZE，触发 5 字节 trailer，
        // 第二条落到 block 1 开头（offset 32768）。
        w.add_record(b"second").unwrap();
        w.flush().unwrap();
        drop(w);

        let bytes = read_file(&path);
        // 第一条占 32763，trailer 5 字节到 block 边界（32768），第二条 13 字节到 32781。
        // 文件不需要对齐到完整 block——第二条只占 block 1 的开头。
        assert_eq!(bytes.len(), 32763 + 5 + HEADER_SIZE + b"second".len());
        // block 0 尾部 5 字节应是 0（trailer）。
        let trailer = &bytes[BLOCK_SIZE - 5..BLOCK_SIZE];
        assert!(trailer.iter().all(|&b| b == 0), "trailer not zero-filled");
        // 第二条 record 从 block 1 起点开始。
        let second_start = BLOCK_SIZE;
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&bytes[second_start..second_start + HEADER_SIZE]);
        let header = decode_header(&header_bytes).unwrap();
        assert_eq!(header.rtype, RecordType::Full);
        assert_eq!(
            &bytes[second_start + HEADER_SIZE..second_start + HEADER_SIZE + 6],
            b"second"
        );
    }

    #[test]
    fn writer_split_preserves_crc_for_each_piece() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        let big = vec![0xCDu8; BLOCK_SIZE * 2];
        w.add_record(&big).unwrap();
        w.flush().unwrap();
        drop(w);

        let bytes = read_file(&path);
        // 验证每个分片的 crc 都正确。
        let mut pos = 0;
        let mut total_data = Vec::new();
        while pos < bytes.len() {
            let block_start = (pos / BLOCK_SIZE) * BLOCK_SIZE;
            let block_end = (block_start + BLOCK_SIZE).min(bytes.len());
            if block_end - pos < HEADER_SIZE {
                pos = block_end;
                continue;
            }
            let mut header_bytes = [0u8; HEADER_SIZE];
            header_bytes.copy_from_slice(&bytes[pos..pos + HEADER_SIZE]);
            let header = match decode_header(&header_bytes) {
                Ok(h) => h,
                Err(_) => {
                    pos = block_end;
                    continue;
                }
            };
            let data = &bytes[pos + HEADER_SIZE..pos + HEADER_SIZE + header.length as usize];
            assert!(
                verify_checksum(header.rtype, data, header.checksum),
                "crc mismatch on a piece"
            );
            total_data.extend_from_slice(data);
            pos += HEADER_SIZE + header.length as usize;
        }
        assert_eq!(total_data, vec![0xCDu8; BLOCK_SIZE * 2]);
    }

    // ===== WalReader 测试 =====

    fn write_records(path: &std::path::Path, records: &[Vec<u8>]) {
        let mut w = WalWriter::create(path).unwrap();
        for r in records {
            w.add_record(r).unwrap();
        }
        w.flush().unwrap();
    }

    #[test]
    fn reader_round_trip_small_records() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let original = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
        write_records(&path, &original);

        let reader = WalReader::open(&path).unwrap();
        let read_back = reader.read_records().unwrap();
        assert_eq!(read_back, original);
    }

    #[test]
    fn reader_round_trip_empty_record() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let original = vec![Vec::new(), b"x".to_vec(), Vec::new()];
        write_records(&path, &original);

        let reader = WalReader::open(&path).unwrap();
        assert_eq!(reader.read_records().unwrap(), original);
    }

    #[test]
    fn reader_round_trip_oversized_split_record() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        // 一条远超 block 的记录 + 几条小记录。
        let original = vec![
            vec![0xABu8; BLOCK_SIZE + 10_000],
            b"after-big".to_vec(),
            vec![0xCDu8; BLOCK_SIZE * 3],
            b"tail".to_vec(),
        ];
        write_records(&path, &original);

        let reader = WalReader::open(&path).unwrap();
        let read_back = reader.read_records().unwrap();
        assert_eq!(read_back, original);
    }

    #[test]
    fn reader_stops_at_corrupted_record() {
        // 写两条完整记录 + 一条会被篡改的记录。
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        write_records(
            &path,
            &[b"first".to_vec(), b"second".to_vec(), b"third".to_vec()],
        );

        // 篡改第三条记录的数据区，使其 crc 失效。
        let mut bytes = read_file(&path);
        // 第三条 record 起点 = 两条小 record 之后。
        let third_data_offset = HEADER_SIZE + 5 + HEADER_SIZE + 6 + HEADER_SIZE;
        bytes[third_data_offset] ^= 0xFF;
        std::fs::write(&path, &bytes).unwrap();

        let reader = WalReader::open(&path).unwrap();
        let read_back = reader.read_records().unwrap();
        // 只返回前两条完整的，第三条因 crc 失败被丢弃。
        assert_eq!(read_back, vec![b"first".to_vec(), b"second".to_vec()]);
    }

    #[test]
    fn reader_handles_trailer_correctly() {
        // 触发 trailer（block 尾填充）后再写记录，reader 必须正确跳过 trailer。
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        let mut w = WalWriter::create(&path).unwrap();
        let first = vec![0x01u8; DATA_MAX - 5];
        w.add_record(&first).unwrap();
        w.add_record(b"after-trailer").unwrap();
        w.flush().unwrap();
        drop(w);

        let reader = WalReader::open(&path).unwrap();
        let read_back = reader.read_records().unwrap();
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0], first);
        assert_eq!(read_back[1], b"after-trailer");
    }

    #[test]
    fn reader_empty_file() {
        let dir = tempfile_dir();
        let path = dir.join("wal.log");
        std::fs::write(&path, b"").unwrap();
        let reader = WalReader::open(&path).unwrap();
        assert!(reader.read_records().unwrap().is_empty());
    }
}
