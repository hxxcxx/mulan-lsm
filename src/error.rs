//! 库的错误类型与统一 `Result` 别名。
//!
//! 存储引擎的失败模式明确且有限，用 `enum` 穷举它们，
//! 调用方能精确 `match` 处理每种情况。

use std::io;

/// mulan-lsm 的统一错误类型。所有公开 API 失败时都返回它。
#[derive(Debug, thiserror::Error)]
pub enum MulanError {
    /// 请求的 key 不存在（未写过，或已被删除）。
    /// 这是 `get` 的正常分支而非故障。
    #[error("key not found")]
    NotFound,

    /// 调用方传入了非法参数（空 key、无效路径等）。
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 数据损坏：CRC 校验失败、文件头/魔数不对、解析出错。
    /// 文件版本不兼容也归入此类（本质都是"文件不符合预期格式"）。
    #[error("data corrupted: {0}")]
    Corrupted(String),

    /// 底层磁盘/文件 IO 错误，由 [`io::Error`] 透传。
    #[error("io error: {0}")]
    Io(#[from] io::Error),

    /// 请求了当前版本尚未实现/不支持的操作。
    #[error("not supported: {0}")]
    NotSupported(String),
}

/// 库内通用的 `Result` 别名，固定错误类型为 [`MulanError`]。
pub type Result<T> = std::result::Result<T, MulanError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_messages_are_human_readable() {
        // 每个变体的 Display 都可读，错误冒泡到顶层时日志才有意义。
        assert_eq!(MulanError::NotFound.to_string(), "key not found");
        assert_eq!(
            MulanError::InvalidArgument("empty key".into()).to_string(),
            "invalid argument: empty key"
        );
        assert_eq!(
            MulanError::Corrupted("bad magic".into()).to_string(),
            "data corrupted: bad magic"
        );
        assert_eq!(
            MulanError::NotSupported("snapshots".into()).to_string(),
            "not supported: snapshots"
        );
    }

    #[test]
    fn io_error_converts_via_from() {
        // #[from] 让 io::Error 能用 `?` 自动转成 MulanError::Io。
        let io_err = io::Error::new(io::ErrorKind::NotFound, "file missing");
        let mulan_err: MulanError = io_err.into();
        assert!(matches!(mulan_err, MulanError::Io(_)));
        assert!(mulan_err.to_string().contains("file missing"));
    }
}
