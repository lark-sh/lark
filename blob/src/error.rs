//! Error types for LarkBlob operations.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum BlobError {
    #[error("invalid magic bytes (expected LARK)")]
    InvalidMagic,

    #[error("unsupported blob version: {0}")]
    UnsupportedVersion(u16),

    #[error("invalid field_id_size flag: {0}")]
    InvalidFieldIdSize(u8),

    #[error("unexpected end of data")]
    UnexpectedEof,

    #[error("unknown node type tag: 0x{0:02x}")]
    UnknownNodeType(u8),

    #[error("field not found in dictionary: {0}")]
    FieldNotFound(String),

    #[error("path not found: {0}")]
    PathNotFound(String),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("node at offset {0} is not a container (type 0x{1:02x})")]
    NotAContainer(u64, u8),

    #[error("field_id {0} out of range for dictionary with {1} entries")]
    FieldIdOutOfRange(u32, u32),

    #[error("dictionary is full (max capacity reached), full recompact needed")]
    DictionaryFull,

    #[error("internal error: {0}")]
    InternalError(String),
}

pub type Result<T> = std::result::Result<T, BlobError>;
