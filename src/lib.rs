pub mod config;
pub mod error;
pub mod fifo;
pub mod http;

use std::{io::Write as _, path::PathBuf};

use futures_util::StreamExt as _;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::{
    config::Config,
    error::{Error, Result},
    fifo::{create_fifo, open_fifo_write},
    http::fetch_stream,
};

/// Mount a single HTTP stream to the FIFO at `config.fifo_path`.
///
/// Runs an indefinite reconnect loop:
///   1. Creates the FIFO (errors if the path already exists on the first
///      iteration; on subsequent iterations the previous FIFO has already been
///      unlinked).
///   2. Waits (via non-blocking poll) until a reader opens the FIFO.
///   3. Initiates the HTTP request.
///   4. Forwards every chunk from the response body into the FIFO.
///   5. When the HTTP stream ends cleanly: closes the write end (the reader
///      receives EOF and stops), unlinks the FIFO, then loops back to step 1
///      so a new reader can open a fresh FIFO and receive the next stream.
///
/// The function never returns `Ok(())`. It only returns on cancellation or
/// error, at which point the FIFO is guaranteed to be unlinked.
///
/// # Errors
///
/// - [`Error::FifoAlreadyExists`] — a filesystem entry already exists at
///   `config.fifo_path` when the first cycle starts.
/// - [`Error::Cancelled`] — `cancel` was cancelled while waiting for a reader
///   or while streaming.
/// - [`Error::Http`] — the HTTP request failed or a chunk could not be read.
/// - [`Error::Io`] — a FIFO write failed.
pub async fn mount(config: Config, cancel: CancellationToken) -> Result<()> {
    tracing::debug!(path = %config.fifo_path.display(), url = %config.url, "mount started");
    loop {
        // Create the FIFO for this cycle.
        let guard = create_fifo(&config.fifo_path)?;

        // Wait for a reader to open the read end.
        tracing::debug!(path = %config.fifo_path.display(), "waiting for reader");
        let mut file = open_fifo_write(&config.fifo_path, &cancel).await?;

        // Establish the HTTP connection.
        tracing::debug!(path = %config.fifo_path.display(), url = %config.url, "reader connected, fetching stream");
        let stream = fetch_stream(&config).await?;
        tokio::pin!(stream);

        // Forward chunks; select! lets cancellation interrupt any chunk wait.
        let clean_end = loop {
            tokio::select! {
                chunk = stream.next() => match chunk {
                    Some(Ok(bytes)) => {
                        // Move `file` into spawn_blocking for the blocking write,
                        // then move it back out to use in the next iteration.
                        tracing::trace!(path = %config.fifo_path.display(), bytes = bytes.len(), "chunk");
                        file = tokio::task::spawn_blocking(move || -> Result<_> {
                            file.write_all(&bytes)?;
                            Ok(file)
                        })
                        .await
                        .map_err(|e| Error::Io(std::io::Error::other(e)))??;
                    }
                    Some(Err(e)) => return Err(e),
                    None => break true,  // stream ended cleanly
                },
                () = cancel.cancelled() => break false,
            }
        };

        // Close the write end first so the reader receives EOF, then unlink
        // the FIFO. Explicit drops make the order clear regardless of
        // declaration order.
        drop(file);
        drop(guard);

        if !clean_end {
            return Err(Error::Cancelled);
        }
        tracing::debug!(path = %config.fifo_path.display(), "stream ended cleanly, restarting");
        // Loop back: create a fresh FIFO and wait for the next reader.
    }
}

/// Mount multiple HTTP streams concurrently, one FIFO per config entry.
///
/// Each mount runs as an independent Tokio task. Results are returned in
/// the same order as the input `configs`.
///
/// With `fail_fast = true` the first mount failure cancels all others via
/// the shared token; remaining mounts return [`Error::Cancelled`].
///
/// With `fail_fast = false` all mounts run to completion independently and
/// all errors are collected.
///
/// A task that panics has its result mapped to [`Error::Io`].
pub async fn mount_all(
    configs: Vec<Config>,
    cancel: CancellationToken,
    fail_fast: bool,
) -> Vec<(PathBuf, Result<()>)> {
    tracing::info!(count = configs.len(), "mounting stream(s)");
    let handles: Vec<(PathBuf, JoinHandle<Result<()>>)> = configs
        .into_iter()
        .map(|config| {
            let path = config.fifo_path.clone();
            let cancel_task = cancel.clone();
            // For fail_fast we need a second clone to call cancel() on failure,
            // because cancel_task is moved into the async block.
            let cancel_fail = cancel.clone();

            let handle = tokio::spawn(async move {
                let result = mount(config, cancel_task).await;
                if fail_fast && result.is_err() {
                    cancel_fail.cancel();
                }
                result
            });

            (path, handle)
        })
        .collect();

    let mut results = Vec::with_capacity(handles.len());
    for (path, handle) in handles {
        let result = handle
            .await
            .unwrap_or_else(|e| Err(Error::Io(std::io::Error::other(e))));
        results.push((path, result));
    }
    results
}
