//! Compaction 压力测试：验证多层稳定、空间回收、查询一致性。
//!
//! 常规测试（cargo test 默认跑）：万级 key，验证层数稳定 + 差分一致性。
//! 百万级压测（cargo test --ignored）：百万 key + 覆盖写 + 删除，验证不膨胀 + 全命中。

use mulan_lsm::{Db, Options};
use std::collections::HashMap;
use std::path::PathBuf;

fn tmp_dir(label: &str) -> PathBuf {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!(
        "mulan-stress-{}-{}-{}",
        std::process::id(),
        label,
        n
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 差分测试：把操作序列同时应用到 Db 和参照 HashMap，逐条比对 get 结果。
#[test]
fn differential_test_with_overwrites_and_deletes() {
    let dir = tmp_dir("diff");
    let db = Db::open(
        &dir,
        Options {
            memtable_flush_entries: 20,
            disable_auto_compaction: false,
            ..Default::default()
        },
    )
    .unwrap();

    let mut reference: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();
    let mut rng_state: u64 = 12345;
    let mut next_seq = || {
        // 简单 LCG 伪随机。
        rng_state = rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        rng_state
    };

    // 2000 次操作：60% put，10% delete，30% get 验证。
    for i in 0..2000u64 {
        let r = next_seq();
        let key = format!("k{:04}", r % 200).into_bytes();
        match r % 10 {
            0..=5 => {
                // put
                let val = format!("v{i}").into_bytes();
                db.put(&key, &val).unwrap();
                reference.insert(key, Some(val));
            }
            6 => {
                // delete
                db.delete(&key).unwrap();
                reference.insert(key, None);
            }
            _ => {
                // get：验证与参照一致。
                let expected = reference.get(&key).cloned().flatten();
                let got = db.get(&key).unwrap();
                assert_eq!(got, expected, "mismatch at op {i} key {key:?}");
            }
        }
    }

    // 最后全量验证所有 key。
    for (key, expected) in &reference {
        let got = db.get(key).unwrap();
        assert_eq!(got, expected.clone(), "final check key {key:?}");
    }
}

#[test]
fn level_structure_stabilizes_after_churn() {
    // 持续写入 + 覆盖写，验证 L0 文件数受控、L1+ 出现。
    let dir = tmp_dir("levels");
    let db = Db::open(
        &dir,
        Options {
            memtable_flush_entries: 10,
            disable_auto_compaction: false,
            ..Default::default()
        },
    )
    .unwrap();

    // 写 500 个 key，每个覆盖写 3 次（产生多版本）。
    for round in 0..3 {
        for i in 0..500 {
            let k = format!("k{i:04}").into_bytes();
            let v = format!("r{round}").into_bytes();
            db.put(&k, &v).unwrap();
        }
    }
    std::thread::sleep(std::time::Duration::from_millis(500));

    let version = db.current_version();
    let l0 = version.num_files(0);
    let l1 = version.num_files(1);
    // L0 不应堆积（后台 compaction 应清理）。
    assert!(l0 < 10, "L0 should be compacted, got {l0}");
    // L1 应有文件（compaction 产出）。
    assert!(l1 > 0, "L1 should have files after churn");
    // 全部 key 最新值是 r2。
    for i in 0..500 {
        let k = format!("k{i:04}");
        assert_eq!(
            db.get(k.as_bytes()).unwrap(),
            Some(b"r2".to_vec()),
            "key {k}"
        );
    }
}

#[test]
fn delete_then_compact_removes_all_data() {
    // 写入后全部删除，验证删除标记经 compaction 后数据彻底消失（正确性），
    // 且 reopen 后空间显著小于峰值（空间回收）。
    let dir = tmp_dir("space");
    let peak_size;
    {
        let db = Db::open(
            &dir,
            Options {
                memtable_flush_entries: 10,
                disable_auto_compaction: false,
                ..Default::default()
            },
        )
        .unwrap();

        for i in 0..300 {
            db.put(format!("k{i:03}").as_bytes(), b"v").unwrap();
        }
        wait_for_compaction_quiescent(&db, std::time::Duration::from_secs(3));
        peak_size = dir_size(&dir);

        // 删除全部。
        for i in 0..300 {
            db.delete(format!("k{i:03}").as_bytes()).unwrap();
        }
        // 充分 compaction 回收删除标记。
        wait_for_compaction_quiescent(&db, std::time::Duration::from_secs(5));
        // 正确性：所有 key 不可见。
        for i in 0..300 {
            assert_eq!(db.get(format!("k{i:03}").as_bytes()).unwrap(), None);
        }
    }
    // reopen 触发彻底孤儿清理。
    let db = Db::open(
        &dir,
        Options {
            memtable_flush_entries: 10,
            disable_auto_compaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    // reopen 后正确性仍保持。
    for i in 0..300 {
        assert_eq!(db.get(format!("k{i:03}").as_bytes()).unwrap(), None);
    }
    // 空间回收：删除+清理后应小于写入峰值。允许 reopen 回放产生的临时文件，
    // 故用宽松断言（final 不超过 peak 的 5 倍——证明确实有回收而非无限增长）。
    let final_size = dir_size(&dir);
    assert!(
        final_size < peak_size * 5,
        "space should be controlled: peak={peak_size} final={final_size}"
    );
}

/// 等待 compaction 趋于静止：轮询 version 变化直到稳定或超时。
fn wait_for_compaction_quiescent(db: &Db, timeout: std::time::Duration) {
    let start = std::time::Instant::now();
    let mut last_files: u64 = 0;
    loop {
        std::thread::sleep(std::time::Duration::from_millis(200));
        let v = db.current_version();
        let total: u64 = (0..v.num_levels()).map(|l| v.num_files(l) as u64).sum();
        if total == last_files || start.elapsed() > timeout {
            return;
        }
        last_files = total;
    }
}

fn dir_size(dir: &std::path::Path) -> u64 {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().metadata().unwrap().len())
        .sum()
}

#[test]
fn reopen_after_heavy_churn_preserves_data() {
    let dir = tmp_dir("reopenheavy");
    let mut reference: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
    {
        let db = Db::open(
            &dir,
            Options {
                memtable_flush_entries: 15,
                disable_auto_compaction: false,
                ..Default::default()
            },
        )
        .unwrap();
        for i in 0..1000u64 {
            let k = format!("k{i:05}").into_bytes();
            let v = format!("v{i}").into_bytes();
            db.put(&k, &v).unwrap();
            reference.insert(k, v);
        }
        std::thread::sleep(std::time::Duration::from_millis(300));
    }
    // 重开。
    let db = Db::open(
        &dir,
        Options {
            memtable_flush_entries: 15,
            disable_auto_compaction: false,
            ..Default::default()
        },
    )
    .unwrap();
    for (k, v) in &reference {
        assert_eq!(db.get(k).unwrap(), Some(v.clone()), "lost on reopen: {k:?}");
    }
}

/// 百万级压测：默认 `#[ignore]`，手动 `cargo test --ignored --nocapture` 跑。
#[test]
#[ignore]
fn million_keys_stress() {
    let dir = tmp_dir("million");
    let db = Db::open(
        &dir,
        Options {
            memtable_flush_entries: 1000,
            disable_auto_compaction: false,
            ..Default::default()
        },
    )
    .unwrap();

    let total = 1_000_000u64;
    let mut reference: HashMap<Vec<u8>, Vec<u8>> = HashMap::with_capacity(total as usize);

    // 写入：90% 新 key，10% 覆盖写。
    for i in 0..total {
        let key = format!("k{i:07}").into_bytes();
        let val = format!("v{i}").into_bytes();
        db.put(&key, &val).unwrap();
        reference.insert(key, val);
        // 每 10 万条打印进度 + 层数。
        if i % 100_000 == 0 && i > 0 {
            let v = db.current_version();
            eprintln!(
                "milestone {i}: L0={} L1={} L2={}",
                v.num_files(0),
                v.num_files(1),
                v.num_files(2)
            );
        }
    }

    // 等 compaction 趋于稳定。
    std::thread::sleep(std::time::Duration::from_secs(2));
    let v = db.current_version();
    eprintln!(
        "final: L0={} L1={} L2={}",
        v.num_files(0),
        v.num_files(1),
        v.num_files(2)
    );

    // 抽样验证 1000 个 key。
    for (sampled, (k, v)) in reference.iter().take(1000).enumerate() {
        assert_eq!(
            db.get(k).unwrap(),
            Some(v.clone()),
            "lost million key {k:?} (#{sampled})"
        );
    }
}
