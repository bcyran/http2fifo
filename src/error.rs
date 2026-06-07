use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The given path already exists (any file type). Never overwritten.
    #[error("path already exists: {0}")]
    FifoAlreadyExists(PathBuf),

    /// An I/O error during FIFO create, open, or write.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A `reqwest` error during request or streaming.
    #[error("request error: {0}")]
    Http(#[from] reqwest::Error),

    /// The operation was cancelled via `CancellationToken`.
    #[error("cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;
