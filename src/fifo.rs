use std::{
    fs::File,
    path::{Path, PathBuf},
    time::Duration,
};

use rustix::{
    fs::{CWD, FileType, Mode, OFlags, fcntl_getfl, fcntl_setfl, mknodat, open},
    io::Errno,
};
use tokio_util::sync::CancellationToken;

use crate::error::{Error, Result};

#[derive(Debug)]
pub struct FifoGuard(PathBuf);

impl Drop for FifoGuard {
    fn drop(&mut self) {
        tracing::debug!(path = %self.0.display(), "unlinking FIFO");
        let _ = std::fs::remove_file(&self.0); // best-effort; silence errors
    }
}

/// Creates a FIFO at `path` (mode `0o600`) and returns a [`FifoGuard`] that
/// will unlink it on drop.
///
/// # Errors
///
/// - [`Error::FifoAlreadyExists`] — any filesystem entry already exists at
///   `path` (regular file, directory, symlink, or existing FIFO).
/// - [`Error::FifoCreate`] — the `mkfifo(2)` syscall failed for any other
///   reason.
pub fn create_fifo(path: &Path) -> Result<FifoGuard> {
    if path.symlink_metadata().is_ok() {
        return Err(Error::FifoAlreadyExists(path.to_owned()));
    }

    mknodat(CWD, path, FileType::Fifo, Mode::RUSR | Mode::WUSR, 0)
        .map_err(|e| Error::FifoCreate(e.into()))?;
    tracing::debug!(path = %path.display(), "FIFO created");
    Ok(FifoGuard(path.to_owned()))
}

/// Opens the FIFO at `path` for writing, polling until a reader appears.
///
/// Opening a FIFO for writing in blocking mode stalls indefinitely until a
/// reader opens the read end — it cannot be interrupted by an async waker.
/// This function polls with `O_NONBLOCK` instead, sleeping 50 ms between
/// attempts, and checks the [`CancellationToken`] on each iteration so the
/// caller can abort cleanly.
///
/// Once the file descriptor is obtained the `O_NONBLOCK` flag is cleared so
/// subsequent writes block and provide natural backpressure.
///
/// Wrapped in [`tokio::task::spawn_blocking`] so the loop does not occupy a
/// Tokio async thread.
///
/// # Errors
///
/// - [`Error::Cancelled`] — the token was cancelled before a reader appeared.
/// - [`Error::Io`] — the `open(2)` or `fcntl(2)` syscall failed, or the
///   [`spawn_blocking`](tokio::task::spawn_blocking) task panicked.
pub async fn open_fifo_write(path: &Path, cancel: &CancellationToken) -> Result<File> {
    let path = path.to_owned();
    let cancel = cancel.clone();

    tokio::task::spawn_blocking(move || open_fifo_write_blocking(&path, &cancel))
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
}

fn open_fifo_write_blocking(path: &Path, cancel: &CancellationToken) -> Result<File> {
    loop {
        match open(path, OFlags::WRONLY | OFlags::NONBLOCK, Mode::empty()) {
            Ok(fd) => {
                // Clear O_NONBLOCK so writes block and provide backpressure.
                let flags = fcntl_getfl(&fd).map_err(|e| Error::Io(e.into()))?;
                fcntl_setfl(&fd, flags & !OFlags::NONBLOCK).map_err(|e| Error::Io(e.into()))?;

                tracing::debug!(path = %path.display(), "write end opened");
                return Ok(File::from(fd));
            }

            Err(Errno::NXIO) => {
                // No reader yet — check for cancellation then wait.
                tracing::trace!(path = %path.display(), "no reader yet, retrying");
                if cancel.is_cancelled() {
                    return Err(Error::Cancelled);
                }
                std::thread::sleep(Duration::from_millis(50));
            }

            Err(e) => {
                return Err(Error::Io(e.into()));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::FileTypeExt;
    use std::time::Duration;

    use rustix::fs::{Mode, OFlags, open};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;

    use super::*;

    // Helper: a unique FIFO path inside a fresh temp directory.
    fn tmp() -> (TempDir, std::path::PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.fifo");
        (dir, path)
    }

    #[test]
    fn create_fifo_makes_fifo() {
        let (_dir, path) = tmp();

        let _guard = create_fifo(&path).expect("create_fifo failed");

        let meta = fs::symlink_metadata(&path).expect("path should exist");
        assert!(
            meta.file_type().is_fifo(),
            "expected a FIFO, got {:?}",
            meta.file_type()
        );
    }

    #[test]
    fn create_fifo_guard_unlinks_on_drop() {
        let (_dir, path) = tmp();

        let guard = create_fifo(&path).expect("create_fifo failed");
        assert!(path.exists(), "FIFO should exist before drop");

        drop(guard);
        assert!(!path.exists(), "FIFO should be gone after drop");
    }

    #[test]
    fn create_fifo_double_create_errors() {
        let (_dir, path) = tmp();

        let _guard = create_fifo(&path).expect("first create_fifo failed");
        let err = create_fifo(&path).expect_err("second create_fifo should fail");

        assert!(
            matches!(err, Error::FifoAlreadyExists(_)),
            "expected FifoAlreadyExists, got {err}"
        );
    }

    #[test]
    fn create_fifo_regular_file_conflict() {
        let (_dir, path) = tmp();

        fs::write(&path, b"").expect("failed to create regular file");
        let err = create_fifo(&path).expect_err("create_fifo on existing file should fail");

        assert!(
            matches!(err, Error::FifoAlreadyExists(_)),
            "expected FifoAlreadyExists, got {err}"
        );
    }

    #[tokio::test]
    async fn open_fifo_write_succeeds_when_reader_present() {
        let (_dir, path) = tmp();
        let _guard = create_fifo(&path).unwrap();
        let cancel = CancellationToken::new();

        // Open the read end with O_NONBLOCK so it succeeds immediately without
        // waiting for a writer, giving open_fifo_write a reader to find.
        let rfd = open(&path, OFlags::RDONLY | OFlags::NONBLOCK, Mode::empty())
            .expect("open read end failed");
        let _reader = fs::File::from(rfd);

        let result = open_fifo_write(&path, &cancel).await;
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[tokio::test]
    async fn open_fifo_write_cancelled_while_waiting() {
        let (_dir, path) = tmp();
        let _guard = create_fifo(&path).unwrap();
        let cancel = CancellationToken::new();

        // Cancel after 100 ms — no reader will ever appear.
        let token = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            token.cancel();
        });

        let result = open_fifo_write(&path, &cancel).await;
        assert!(
            matches!(result, Err(Error::Cancelled)),
            "expected Cancelled, got {result:?}"
        );
    }
}
