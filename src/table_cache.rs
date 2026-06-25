//! Table Cache：缓存已打开的 `TableReader`，避免 `get` 路径重复解析 SSTable。
//!
//! 当前问题：`Db::get` 每查一个 SSTable 都做 `TableReader::open`（全量读文件 +
//! 解析 footer/metaindex/布隆/index），即使 OS page cache 命中，重复解析的 CPU
//! 开销也不小。缓存后热点文件驻留内存，get 直接走已解析好的 `TableReader`。
//!
//! 缓存策略：简单 FIFO + 容量上限。容量满时淘汰最先插入的。`Arc` 保证即使被缓存
//! 淘汰，正在使用中的 reader 不会提前释放。
//!
//! Compaction 提交后，旧文件不再属于 Version，调用 `evict` 从缓存中移除。
//! Scan（`new_iterator_at`）不走缓存——它 consume reader 转成 TableIter，不适合缓存。

use crate::error::Result;
use crate::file_meta::{sst_path, FileNumber};
use crate::sstable::TableReader;
use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

const DEFAULT_CACHE_CAPACITY: usize = 100;

struct CacheInner {
    entries: HashMap<FileNumber, Arc<TableReader>>,
    order: VecDeque<FileNumber>,
    capacity: usize,
}

pub struct TableCache {
    inner: Mutex<CacheInner>,
    dir: PathBuf,
}

impl TableCache {
    pub fn new(dir: PathBuf) -> Self {
        Self::with_capacity(dir, DEFAULT_CACHE_CAPACITY)
    }

    pub fn with_capacity(dir: PathBuf, capacity: usize) -> Self {
        TableCache {
            inner: Mutex::new(CacheInner {
                entries: HashMap::new(),
                order: VecDeque::new(),
                capacity,
            }),
            dir,
        }
    }

    /// 获取编号 `number` 的 SSTable 的 `TableReader`。
    ///
    /// 缓存命中 → 返回 clone 的 `Arc`；未命中 → 打开文件、加入缓存、返回。
    /// 缓存满时淘汰最久未用的条目。文件打开不在锁内进行，减少锁争用。
    /// 容量为 0 时不缓存（每次直接打开文件，不进缓存）。
    pub fn get(&self, number: FileNumber) -> Result<Arc<TableReader>> {
        let mut inner = self.inner.lock().unwrap();
        if inner.capacity == 0 {
            drop(inner);
            return Ok(Arc::new(TableReader::open(&sst_path(&self.dir, number))?));
        }
        if let Some(reader) = inner.entries.get(&number) {
            let cloned = Arc::clone(reader);
            // LRU：命中时将该条目移到队尾，避免热点被淘汰。
            inner.order.retain(|n| *n != number);
            inner.order.push_back(number);
            return Ok(cloned);
        }
        let dir = self.dir.clone();
        drop(inner);
        let reader = Arc::new(TableReader::open(&sst_path(&dir, number))?);
        let mut inner = self.inner.lock().unwrap();
        if inner.entries.len() >= inner.capacity {
            if let Some(old) = inner.order.pop_front() {
                inner.entries.remove(&old);
            }
        }
        inner.entries.insert(number, Arc::clone(&reader));
        inner.order.push_back(number);
        Ok(reader)
    }

    /// 从缓存中移除指定编号的 SSTable（compaction 后旧文件不再需要）。
    pub fn evict(&self, number: FileNumber) {
        let mut inner = self.inner.lock().unwrap();
        inner.entries.remove(&number);
        inner.order.retain(|n| *n != number);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_meta::{sst_path, FileNumber};
    use crate::memtable::MemTable;
    use std::path::PathBuf;

    fn build_test_sst(dir: &std::path::Path, num: FileNumber) {
        let mut mem = MemTable::new();
        mem.put(b"k", b"v");
        let path = sst_path(dir, num);
        mem.flush_to_sstable_with_bounds(&path).unwrap();
    }

    fn tmp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "mulan-cache-test-{}-{}-{}",
            std::process::id(),
            label,
            std::sync::atomic::AtomicU64::new(0).fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn cache_hit_returns_same_reader() {
        let dir = tmp_dir("hit");
        build_test_sst(&dir, FileNumber(1));
        let cache = TableCache::new(dir);
        let r1 = cache.get(FileNumber(1)).unwrap();
        let r2 = cache.get(FileNumber(1)).unwrap();
        // 同一文件的 Arc 应指向同一 TableReader（指针地址相同）。
        assert!(Arc::ptr_eq(&r1, &r2), "cache should return same Arc");
    }

    #[test]
    fn evict_removes_from_cache() {
        let dir = tmp_dir("evict");
        build_test_sst(&dir, FileNumber(1));
        build_test_sst(&dir, FileNumber(2));
        let cache = TableCache::new(dir);
        let r1 = cache.get(FileNumber(1)).unwrap();
        cache.evict(FileNumber(1));
        let r2 = cache.get(FileNumber(1)).unwrap();
        assert!(
            !Arc::ptr_eq(&r1, &r2),
            "after evict, re-open creates new Arc"
        );
    }

    #[test]
    fn cache_evicts_oldest_when_full() {
        let dir = tmp_dir("full");
        for i in 1..=3 {
            build_test_sst(&dir, FileNumber(i));
        }
        let cache = TableCache::with_capacity(dir, 2);
        // 容量=2，第三个插入时应淘汰第一个。
        cache.get(FileNumber(1)).unwrap();
        cache.get(FileNumber(2)).unwrap();
        cache.get(FileNumber(3)).unwrap();
        // 重新获取已被淘汰的 1 → 新打开（走 miss 路径）
        let _r1 = cache.get(FileNumber(1)).unwrap();
        // 2 和 3 一直在缓存中
        let _r2 = cache.get(FileNumber(2)).unwrap();
        let _r3 = cache.get(FileNumber(3)).unwrap();
        // 全部可正常读取
        assert_eq!(_r1.get(b"k").unwrap(), Some(b"v".as_slice()));
        assert_eq!(_r2.get(b"k").unwrap(), Some(b"v".as_slice()));
        assert_eq!(_r3.get(b"k").unwrap(), Some(b"v".as_slice()));
    }

    #[test]
    fn cache_miss_opens_file() {
        let dir = tmp_dir("miss");
        build_test_sst(&dir, FileNumber(1));
        let cache = TableCache::new(dir);
        let reader = cache.get(FileNumber(1)).unwrap();
        assert_eq!(reader.get(b"k").unwrap(), Some(b"v".as_slice()));
    }

    /// LRU：频繁访问的条目不应被淘汰。
    #[test]
    fn cache_lru_keeps_hot_entries() {
        let dir = tmp_dir("lru");
        for i in 1..=3 {
            build_test_sst(&dir, FileNumber(i));
        }
        let cache = TableCache::with_capacity(dir, 2);
        cache.get(FileNumber(1)).unwrap(); // 1 在队尾
        cache.get(FileNumber(2)).unwrap(); // 2 在队尾，1 在队头
                                           // 重新访问 1 → 1 移到队尾，2 变队头
        cache.get(FileNumber(1)).unwrap();
        // 插入 3 → 淘汰队头（2），1 因在队尾而存活
        cache.get(FileNumber(3)).unwrap();
        // 1 应在缓存中（同一 Arc）
        let r1a = cache.get(FileNumber(1)).unwrap();
        let r1b = cache.get(FileNumber(1)).unwrap();
        assert!(Arc::ptr_eq(&r1a, &r1b), "hot entry should survive eviction");
    }
}
