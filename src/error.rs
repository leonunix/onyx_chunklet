use std::path::PathBuf;

use thiserror::Error;

pub type ChunkletResult<T> = Result<T, ChunkletError>;

#[derive(Debug, Error)]
pub enum ChunkletError {
    #[error("device {path}: {reason}")]
    Device { path: PathBuf, reason: String },

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("config: {0}")]
    Config(String),

    #[error("on-disk format: {0}")]
    Format(String),

    #[error("crc mismatch in {what}: stored={stored:08x} computed={computed:08x}")]
    Crc {
        what: String,
        stored: u32,
        computed: u32,
    },

    #[error("no valid superblock on {path}")]
    NoValidSuperblock { path: PathBuf },

    #[error("pool inconsistency: {0}")]
    PoolMismatch(String),

    #[error("not enough free chunklets on PD {pd:?}: need {need}, have {have}")]
    NoSpace {
        pd: crate::types::PdId,
        need: u32,
        have: u32,
    },

    #[error("invariant violated: {0}")]
    Invariant(String),

    #[error("unsupported: {0}")]
    Unsupported(String),
}
