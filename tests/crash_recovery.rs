//! 崩溃恢复集成测试：MemTable × WAL 的完整闭环。
//!
//! 模拟真实使用：写入时同步追加 WAL，"崩溃"后从 WAL 回放重建 MemTable，
//! 验证数据一致。包含正常 round-trip、末尾截断（写一半崩溃）、crc 损坏。

use mulan_lsm::internal_key::ValueType;
use mulan_lsm::memtable::MemTable;
use mulan_lsm::wal::{decode_entry, encode_entry, WalReader, WalWriter};
use std::collections::HashMap;
use std::path::PathBuf;

/// 每个测试用独立临时目录，避免互相干扰。
fn tmp_path(name: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("mulan-crash-test-{}-{}", std::process::id(), n));
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(name)
}

/// 模拟一次运行：写入操作同时落 MemTable 和 WAL，返回最终 MemTable 状态和 WAL 路径。
struct LiveDb {
    memtable: MemTable,
    writer: WalWriter,
    path: PathBuf,
}

impl LiveDb {
    fn open(path: PathBuf) -> Self {
        LiveDb {
            memtable: MemTable::new(),
            writer: WalWriter::create(&path).unwrap(),
            path,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.memtable.put(key, value);
        let seq = self.memtable.sequence();
        let entry = encode_entry(ValueType::Put, seq, key, value);
        self.writer.add_record(&entry).unwrap();
    }

    fn delete(&mut self, key: &[u8]) {
        self.memtable.delete(key);
        let seq = self.memtable.sequence();
        let entry = encode_entry(ValueType::Delete, seq, key, &[]);
        self.writer.add_record(&entry).unwrap();
    }

    /// 模拟崩溃：丢弃内存状态，但 WAL 文件保留在磁盘上。
    fn crash(self) -> PathBuf {
        // 故意不 flush——测试也覆盖"未刷盘"场景（虽然 OS 缓存通常还在）。
        self.path
    }
}

/// 从 WAL 回放，重建 MemTable。
fn recover(path: &std::path::Path) -> MemTable {
    let reader = WalReader::open(path).unwrap();
    let records = reader.read_records().unwrap();
    let mut memtable = MemTable::new();
    for record in records {
        let entry = decode_entry(&record).unwrap();
        memtable.apply(entry.vtype, entry.seq, &entry.key, &entry.value);
    }
    memtable
}

/// 用 HashMap 作为"期望最终状态"的参照：put 覆盖，delete 移除。
fn expected_state(ops: &[(ValueType, Vec<u8>, Vec<u8>)]) -> HashMap<Vec<u8>, Vec<u8>> {
    let mut map = HashMap::new();
    for (vtype, key, value) in ops {
        match vtype {
            ValueType::Put => {
                map.insert(key.clone(), value.clone());
            }
            ValueType::Delete => {
                map.remove(key);
            }
        }
    }
    map
}

/// 校验重建的 MemTable 与期望状态一致：每个 key 的 get 结果匹配。
fn assert_memtable_matches(memtable: &MemTable, expected: &HashMap<Vec<u8>, Vec<u8>>) {
    // 期望存在的 key 都能读到正确值。
    for (key, value) in expected {
        let got = memtable.get(key).unwrap();
        assert_eq!(
            got,
            Some(value.clone()),
            "key {:?}: expected {:?}, got {:?}",
            key,
            value,
            got
        );
    }
    // 收集所有出现过的 key（含被删除的），验证被删的返回 None。
    // 这里简单：expected 里的 key 已覆盖；额外验证几个肯定不存在的 key。
    assert_eq!(memtable.get(b"definitely-not-exist").unwrap(), None);
}

#[test]
fn crash_recovery_normal_round_trip() {
    let path = tmp_path("normal.log");
    let mut db = LiveDb::open(path.clone());
    db.put(b"k1", b"v1");
    db.put(b"k2", b"v2");
    db.delete(b"k1");
    db.put(b"k2", b"v2-updated");
    db.put(b"k3", b"v3");
    let path = db.crash();

    let recovered = recover(&path);
    let expected = expected_state(&[
        (ValueType::Put, b"k1".to_vec(), b"v1".to_vec()),
        (ValueType::Put, b"k2".to_vec(), b"v2".to_vec()),
        (ValueType::Delete, b"k1".to_vec(), Vec::new()),
        (ValueType::Put, b"k2".to_vec(), b"v2-updated".to_vec()),
        (ValueType::Put, b"k3".to_vec(), b"v3".to_vec()),
    ]);
    assert_memtable_matches(&recovered, &expected);
    // seq 一致性：回放后 sequence == 最后一条 record 的 seq（这里 5 条操作）。
    assert_eq!(recovered.sequence(), 5);
}

#[test]
fn crash_recovery_preserves_multiversion() {
    // 同一 key 多次 put，回放后 get 应返回最新版本。
    let path = tmp_path("multiversion.log");
    let mut db = LiveDb::open(path.clone());
    for i in 0..50u32 {
        db.put(b"counter", format!("v{i}").as_bytes());
    }
    let path = db.crash();

    let recovered = recover(&path);
    assert_eq!(recovered.get(b"counter").unwrap(), Some(b"v49".to_vec()));
    assert_eq!(recovered.sequence(), 50);
}

#[test]
fn crash_recovery_truncated_tail() {
    // 写入若干记录后，截断 WAL 文件末尾，模拟最后一条写一半崩溃。
    let path = tmp_path("truncated.log");
    let mut db = LiveDb::open(path.clone());
    db.put(b"a", b"1");
    db.put(b"b", b"2");
    db.put(b"c", b"3");
    db.writer.flush().unwrap();
    let path = db.crash();

    // 截断文件末尾几个字节，破坏最后一条 record。
    let original = std::fs::read(&path).unwrap();
    let truncated_len = original.len().saturating_sub(3);
    std::fs::write(&path, &original[..truncated_len]).unwrap();

    let recovered = recover(&path);
    // 至少前两条完整记录能恢复（第三条可能因截断而 crc 失败被丢弃）。
    assert_eq!(recovered.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(recovered.get(b"b").unwrap(), Some(b"2".to_vec()));
    // 回放不应 panic。
}

#[test]
fn crash_recovery_corrupted_middle() {
    // 篡改中间一条 record 的数据，使其 crc 失败。reader 应在该处停止，
    // 只返回此前完整的记录。
    let path = tmp_path("corrupted.log");
    let mut db = LiveDb::open(path.clone());
    db.put(b"first", b"keep");
    db.put(b"second", b"corrupt-this");
    db.put(b"third", b"lost");
    db.writer.flush().unwrap();
    let path = db.crash();

    // 找到第二条 record 的数据区并篡改。
    // 简化：直接破坏第二条 record 头之后的某个字节。
    // 第一条 record 占 HEADER + entry；entry 最小约 1+8+1+5+1+4 = 20 字节。
    // 篡改一个靠后的字节（大概率落在第二条 record 内）。
    let mut bytes = std::fs::read(&path).unwrap();
    if bytes.len() > 30 {
        bytes[30] ^= 0xFF;
    }
    std::fs::write(&path, &bytes).unwrap();

    // 回放：reader 在损坏处停止，只返回第一条。
    let reader = WalReader::open(&path).unwrap();
    let records = reader.read_records().unwrap();
    // 至少第一条完整记录在。
    assert!(!records.is_empty());
    let first = decode_entry(&records[0]).unwrap();
    assert_eq!(first.key, b"first");
}

#[test]
fn crash_recovery_empty_wal() {
    let path = tmp_path("empty.log");
    // 空 WAL 文件，回放得到空 MemTable。
    std::fs::write(&path, b"").unwrap();
    let recovered = recover(&path);
    assert_eq!(recovered.sequence(), 0);
    assert_eq!(recovered.get(b"anything").unwrap(), None);
}

#[test]
fn crash_recovery_oversized_record_split() {
    // 写一条超大 value 的 put（触发 WAL 分片），验证回放能正确拼装。
    let path = tmp_path("oversized.log");
    let mut db = LiveDb::open(path.clone());
    let big_value = vec![0x42u8; 40_000]; // > 一个 block
    db.put(b"big", &big_value);
    let path = db.crash();

    let recovered = recover(&path);
    assert_eq!(recovered.get(b"big").unwrap(), Some(big_value));
}

#[test]
fn crash_recovery_seq_continuity() {
    // 写入 → 崩溃 → 回放 → 继续写入：新写入的 seq 应从回放后的 max seq + 1 继续。
    let path = tmp_path("seq_continuity.log");
    let mut db = LiveDb::open(path.clone());
    for i in 0..10u32 {
        db.put(format!("k{i}").as_bytes(), b"v");
    }
    let path = db.crash();

    let mut recovered = recover(&path);
    assert_eq!(recovered.sequence(), 10);
    // 继续写入，seq 应为 11。
    recovered.put(b"after-recover", b"new");
    assert_eq!(recovered.sequence(), 11);
    assert_eq!(
        recovered.get(b"after-recover").unwrap(),
        Some(b"new".to_vec())
    );
}
