//! mulan-lsm：Rust 实现的 LSM-Tree 键值存储。

pub mod bloom;
pub mod compaction;
pub mod db;
pub mod error;
pub mod file_meta;
pub mod internal_key;
pub mod iterator;
pub mod manifest;
pub mod memtable;
pub mod skiplist;
pub mod sstable;
pub mod varint;
pub mod version;
pub mod wal;

pub use db::{Db, Options};
pub use error::{MulanError, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_skeleton_works() {
        assert_eq!(2 + 2, 4);
    }
}
