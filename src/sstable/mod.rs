//! SSTable：LSM 的磁盘只读有序表。

pub mod block;

pub use block::{Block, BlockBuilder};
