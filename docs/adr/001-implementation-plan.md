# 001 — Implementation Plan: `http2fifo`

## Purpose

`http2fifo` is a Rust crate that mounts an HTTP streaming resource as a Unix
named pipe (FIFO). A reader process opens the FIFO as if it were a local file;
`http2fifo` transparently fetches the HTTP stream and forwards every byte chunk
into the pipe. The crate exposes both a reusable library and a CLI binary.

---

## Design Decisions

| Concern | Decision | Rationale |
|---|---|---|
| HTTP stream type | Any streaming response body | Most general; SSE is a subset |
| Reconnection | Exit on stream end | Simple for v1; backoff deferred to v2 |
| FIFO lifecycle | Tool creates on start, unlinks on all exit paths | Ergonomic; no leftover files |
| Backpressure | Block the HTTP read | No data loss; FIFO pipe buffer provides natural flow control |
| Async runtime | Tokio | Best ecosystem fit with `reqwest` and `clap` |
| HTTP method | Configurable per invocation (`-X`) | Supports POST/PUT streaming endpoints |
| Signal handling | SIGINT + SIGTERM cancel all mounts and unlink all FIFOs | Clean Ctrl-C behaviour |
| Path conflict | `FifoAlreadyExists` error — never overwrite | Safety; prevents accidental clobbering |
| Per-mount options | Global flags apply to all mounts | Sufficient for v1; per-mount config deferred to v2 |

---

## Repository Layout

```
src/
  lib.rs          — public API: re-exports, mount(), mount_all()
  config.rs       — Config struct
  error.rs        — Error enum
  fifo.rs         — FIFO lifecycle + RAII guard
  http.rs         — reqwest streaming fetch
  main.rs         — CLI binary
tests/
  integration.rs  — end-to-end tests against an in-process mock HTTP server
docs/
  adr/001-implementation-plan.md   — this document
```

---

## Dependencies

### Runtime

| Crate | Features | Purpose |
|---|---|---|
| `tokio` | `full` | Async runtime, `spawn_blocking`, signal handling |
| `reqwest` | `stream` | HTTP client with streaming response body |
| `clap` | `derive` | CLI argument parsing with derive macros |
| `nix` | `fs` | `mkfifo(2)` syscall |
| `thiserror` | — | `#[derive(Error)]` for the error enum |
| `futures-util` | — | `StreamExt::next()` for iterating response body chunks |
| `bytes` | — | `Bytes` chunk type shared between reqwest and FIFO writer |
| `tokio-util` | `sync` | `CancellationToken` for cooperative, multi-task shutdown |

### Dev (integration tests only)

| Crate | Features | Purpose |
|---|---|---|
| `wiremock` | — | Declarative mock HTTP server; binds to a random OS-assigned port |

---

## Public Library API

```rust
/// Mount a single HTTP stream to the FIFO at `config.fifo_path`.
///
/// Execution order:
///   1. Creates the FIFO (errors if the path already exists).
///   2. Waits (non-blocking poll) until a reader opens the FIFO.
///   3. Initiates the HTTP request.
///   4. Forwards every chunk from the response body into the FIFO.
///   5. Unlinks the FIFO on all exit paths via an RAII guard.
///
/// Returns `Ok(())` when the HTTP stream ends cleanly.
/// Returns `Err(Error::Cancelled)` if the token is cancelled.
pub async fn mount(config: Config, cancel: CancellationToken) -> Result<()>;

/// Mount multiple HTTP streams concurrently, one FIFO per config entry.
///
/// Each mount runs as an independent `tokio` task.
///
/// With `fail_fast = true`:
///   The first mount failure cancels all others via the shared token.
///
/// With `fail_fast = false`:
///   All mounts run to completion independently; errors are collected.
///
/// Returns one result per config, keyed by FIFO path for clear error reporting.
pub async fn mount_all(
    configs: Vec<Config>,
    cancel: CancellationToken,
    fail_fast: bool,
) -> Vec<(PathBuf, Result<()>)>;
```

---

## Atomic Implementation Steps

### `[ ]` Step 1 — `Cargo.toml`

Scaffold the crate manifest:

- `[package]` with `name = "http2fifo"`, `edition = "2021"`
- `[lib]` target (`src/lib.rs`)
- `[[bin]]` named `http2fifo` (`src/main.rs`)
- `[dependencies]` — all runtime crates listed above
- `[dev-dependencies]` — `wiremock` for integration tests

---

### `[ ]` Step 2 — `src/error.rs`

Define the crate-wide error type and a `Result<T>` alias:

```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The given path already exists (any file type). Never overwritten.
    #[error("path already exists: {0}")]
    FifoAlreadyExists(PathBuf),

    /// The `mkfifo(2)` syscall failed.
    #[error("failed to create FIFO: {0}")]
    FifoCreate(nix::Error),

    /// An I/O error during FIFO open or write.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// A `reqwest` error during HTTP request or streaming.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// The operation was cancelled via `CancellationToken`.
    #[error("cancelled")]
    Cancelled,
}

pub type Result<T> = std::result::Result<T, Error>;
```

---

### `[ ]` Step 3 — `src/config.rs`

Plain data struct; no validation logic here (validation belongs at the call site):

```rust
pub struct Config {
    /// HTTP endpoint to stream from.
    pub url: String,

    /// HTTP method (GET, POST, PUT, …).
    pub method: reqwest::Method,

    /// Optional request body, used with POST/PUT/PATCH.
    pub body: Option<bytes::Bytes>,

    /// Additional request headers as `(name, value)` pairs.
    pub headers: Vec<(String, String)>,

    /// Filesystem path where the FIFO will be created.
    pub fifo_path: std::path::PathBuf,
}
```

---

### `[ ]` Step 4 — `src/fifo.rs`

Three responsibilities: creation, cleanup, and opening for write.

#### `create_fifo(&Path) -> Result<FifoGuard>`

1. Call `Path::symlink_metadata()`: if the path exists under any form
   (regular file, directory, symlink, existing FIFO), return
   `Error::FifoAlreadyExists`.
2. Call `nix::unistd::mkfifo(path, Mode::S_IRUSR | Mode::S_IWUSR)`
   (mode `0o600`); wrap any error as `Error::FifoCreate`.
3. Return `FifoGuard(path.to_owned())`.

#### `FifoGuard(PathBuf)`

RAII cleanup struct:

```rust
pub struct FifoGuard(PathBuf);

impl Drop for FifoGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0); // best-effort; silence errors
    }
}
```

Not `Clone`. Ownership guarantees exactly one cleanup regardless of how the
calling code exits (return, `?`, panic).

#### `open_fifo_write(&Path, &CancellationToken) -> Result<File>`

Opening a FIFO for writing in blocking mode stalls indefinitely until a reader
appears — it cannot be interrupted by a signal or async cancellation. To avoid
leaking a thread, this function polls instead:

```
loop:
  attempt open(path, O_WRONLY | O_NONBLOCK)
  ├─ Ok(fd)   → clear O_NONBLOCK via fcntl(F_SETFL, flags & ~O_NONBLOCK)
  │             so subsequent writes block and provide backpressure
  │             return Ok(File::from_raw_fd(fd))
  ├─ Err(ENXIO) → no reader yet
  │               check CancellationToken — if cancelled, return Err(Cancelled)
  │               sleep 50 ms
  │               continue loop
  └─ Err(e)   → return Err(Error::Io(e))
```

Wrapped in `tokio::task::spawn_blocking` so the polling loop does not block
the Tokio thread pool.

---

### `[ ]` Step 5 — `src/http.rs`

```rust
pub async fn fetch_stream(
    config: &Config,
) -> Result<impl Stream<Item = Result<bytes::Bytes>>>
```

Implementation:

1. Construct `reqwest::Client::new()`.
2. Begin building a `RequestBuilder` with `client.request(config.method.clone(), &config.url)`.
3. Add each `(name, value)` pair from `config.headers` via `.header()`.
4. If `config.body.is_some()`, set it via `.body()`.
5. `.send().await?` — propagates connection / DNS errors as `Error::Http`.
6. Assert a 2xx status; return a non-2xx as `Error::Http` via `error_for_status()`.
7. Return `response.bytes_stream().map(|r| r.map_err(Error::Http))`.

---

### `[ ]` Step 6 — `src/lib.rs`

#### `mount(config: Config, cancel: CancellationToken) -> Result<()>`

```
1.  _guard  = create_fifo(&config.fifo_path)?
              // FIFO exists on disk from this point; guard unlinks on drop

2.  file    = open_fifo_write(&config.fifo_path, &cancel).await?
              // blocks (via polling) until a reader opens the read end,
              // or returns Err(Cancelled)

3.  stream  = fetch_stream(&config).await?
              // HTTP connection established

4.  loop:
      select! {
        chunk = stream.next() => match chunk {
          Some(Ok(bytes))  => spawn_blocking(|| file.write_all(&bytes)).await??
                              // blocking write; stalls if FIFO buffer full (backpressure)
          Some(Err(e))     => return Err(e)
          None             => return Ok(())   // stream ended cleanly
        }
        _ = cancel.cancelled() => return Err(Error::Cancelled)
      }

5.  // _guard drops here (and on every early-return path above) → FIFO unlinked
```

#### `mount_all(configs, cancel, fail_fast) -> Vec<(PathBuf, Result<()>)>`

1. For each `config`, clone `cancel` and spawn:
   ```rust
   tokio::spawn(mount(config, cancel_clone))
   ```
2. Collect handles in a `JoinSet`.
3. With `fail_fast = true`: after each task completes, if its result is `Err`,
   call `cancel.cancel()` to abort remaining tasks.
4. Await all handles; map `JoinError` (panic) to `Error::Io`.
5. Return `Vec<(PathBuf, Result<()>)>` preserving insertion order.

---

### `[ ]` Step 7 — `src/main.rs`

#### CLI interface

```
USAGE:
    # Single mount — positional convenience alias
    http2fifo [OPTIONS] <URL> <FIFO_PATH>

    # Multi-mount — explicit flags (one --mount per pair)
    http2fifo [OPTIONS] --mount <URL> <FIFO_PATH> [--mount <URL> <FIFO_PATH> …]

ARGUMENTS:
    <URL>           HTTP endpoint to stream (single-mount form)
    <FIFO_PATH>     FIFO path to create (single-mount form)

OPTIONS:
    -X, --method <METHOD>       HTTP method applied to all mounts [default: GET]
    -H, --header <NAME:VALUE>   Request header applied to all mounts (repeatable)
    -d, --data <BODY>           Request body (UTF-8) applied to all mounts
        --fail-fast             Cancel all mounts if any one fails
    -h, --help
    -V, --version
```

Positionals and `--mount` are mutually exclusive. Validated at runtime (not at
the clap struct level) with a descriptive error message such as:
`"error: cannot combine positional <URL>/<FIFO_PATH> with --mount flags"`.

#### `--header` parsing

Each `-H "Name: Value"` or `-H "Name:Value"` is split on the first `:`,
trimming whitespace from both parts. An entry with no `:` is rejected with a
clear error.

#### Signal handling

```rust
let token  = CancellationToken::new();
let t2     = token.clone();

tokio::spawn(async move {
    let mut sigint  = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::select! {
        _ = sigint.recv()  => {},
        _ = sigterm.recv() => {},
    }
    t2.cancel();
});
```

The main task races `mount` / `mount_all` against `token.cancelled()`.

#### Exit codes

| Condition | Code |
|---|---|
| All mounts ended cleanly | `0` |
| One or more mounts errored | `1` |
| Terminated by SIGINT | `130` |
| Terminated by SIGTERM | `143` |

---

### `[ ]` Step 8 — Unit tests (`src/fifo.rs`, `#[cfg(test)]`)

All tests use `tempfile::TempDir` (or a manually constructed temp path) so
they are isolated and leave no artefacts.

| Test name | What it checks |
|---|---|
| `create_makes_fifo` | Path exists after `create_fifo`; `metadata().file_type().is_fifo()` is true |
| `guard_unlinks_on_drop` | Path is gone after `FifoGuard` is dropped |
| `double_create_errors` | Second `create_fifo` on same path returns `FifoAlreadyExists` |
| `regular_file_conflict` | `create_fifo` on a path pre-occupied by a regular file returns `FifoAlreadyExists` |

---

### `[ ]` Step 9 — Integration tests (`tests/integration.rs`)

All tests use `wiremock::MockServer::start().await`, which binds to
`127.0.0.1` on an OS-assigned port. Mocks are registered with
`Mock::given(…).respond_with(ResponseTemplate::new(200).set_body_bytes(…))`.
No external network access or manual server setup required.

For the cancellation test, the mock serves a very large body (e.g. 10 MB of
zeros) so the transfer cannot complete within the 100 ms cancellation window.

| Test name | Setup | Assertion |
|---|---|---|
| `happy_path` | Mock returns a fixed payload as the response body | Bytes collected from FIFO equal the full payload |
| `cancellation` | Mock returns a 10 MB body; token cancelled after 100 ms | `mount()` returns `Err(Cancelled)`; FIFO path no longer exists on disk |
| `multi_mount` | Two `MockServer` instances with distinct payloads; two FIFOs; background readers on both | Both readers receive their respective payloads correctly |
| `fail_fast` | Mount A mock returns 500; Mount B mock returns a 10 MB body | Both mounts terminate; Mount A result is `Err(Http(…))`; Mount B result is `Err(Cancelled)` |

---

## Deferred to v2

- **Reconnection** — retry with exponential backoff when the HTTP stream drops
- **Per-mount options** — different method/headers/body per `--mount` entry,
  expressed via `--config <file>` (TOML)
- **`@filename` body** — read request body from a file (`-d @path`), curl-style
- **`--timeout` / `--connect-timeout`** — per-request deadline flags
- **TLS client certificates** — mutual TLS for authenticated streaming endpoints
- **HTTP/2 support** — enable `reqwest`'s `http2` feature for multiplexed streams
