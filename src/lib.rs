//! mulan-lsm：Rust 实现的 LSM-Tree 键值存储。

pub mod error;
pub mod internal_key;
pub mod memtable;
pub mod skiplist;

pub use error::{MulanError, Result};

#[cfg(test)]
mod tests {
    #[test]
    fn placeholder_skeleton_works() {
        assert_eq!(2 + 2, 4);
    }
}
