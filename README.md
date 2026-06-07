# http2fifo

> **Hobby project.** This was made out of curiosity and for the fun of learning. It is probably not very useful in practice. It was created with significant AI assistance.

A small Rust tool that bridges an HTTP streaming endpoint to a Unix named pipe (FIFO). Any program that can simply `open()` and read a file can consume an HTTP stream — no HTTP knowledge required on the reader's side.

```
reader (cat, mpv, ffmpeg, …)  <──  FIFO  <──  http2fifo  <──  HTTP stream
```

## How it works

`http2fifo` creates a named pipe at a given path, waits for a reader to open it, then fetches the HTTP response and forwards every byte chunk into the pipe. When the stream ends it unlinks the FIFO and loops back, ready to serve the next reader. Cancellation (Ctrl-C / SIGTERM) is handled cleanly — the FIFO is always removed.

Multiple streams can be mounted concurrently with `--mount`.

## Build

Requires Rust 1.96.0 (pinned via `rust-toolchain.toml`; `rustup` installs it automatically). No system OpenSSL needed — TLS is handled by `rustls`.

```bash
cargo build --release
# binary: target/release/http2fifo
```

With Nix:

```bash
nix build
# binary: result/bin/http2fifo
```

Or run directly without installing:

```bash
nix run github:bcyran/http2fifo -- https://ice4.somafm.com/dubstep-256-mp3 /tmp/dubstep.fifo
```

## Usage

### Single stream

```bash
http2fifo <URL> <FIFO_PATH>
```

Start `http2fifo` first, then open the FIFO in a second terminal (or after a short delay) — the FIFO must exist before the reader tries to open it.

```bash
# terminal 1
http2fifo https://ice4.somafm.com/dubstep-256-mp3 /tmp/dubstep.fifo

# terminal 2
mpv /tmp/dubstep.fifo
```

### Multiple concurrent streams

```bash
http2fifo --mount <URL1> <FIFO1> --mount <URL2> <FIFO2>
```

Positional arguments and `--mount` are mutually exclusive.

### Options

| Flag | Short | Description |
|---|---|---|
| `--method METHOD` | `-X` | HTTP method (default: `GET`) |
| `--header NAME:VALUE` | `-H` | Request header, repeatable |
| `--data BODY` | `-d` | Request body |
| `--fail-fast` | | Cancel all mounts if any one fails |
| `--log-level LEVEL` | | `off/error/warn/info/debug/trace`; overrides `RUST_LOG` |

### Exit codes

| Code | Meaning |
|---|---|
| `0` | All streams finished cleanly |
| `1` | One or more errors |
| `130` | Killed by SIGINT |
| `143` | Killed by SIGTERM |

## Development

A `justfile` provides common tasks:

```bash
just check     # fmt check + lint + tests (full CI suite)
just fmt       # auto-format
just test      # run tests
just fix       # fmt + lint --fix
```

CI runs `just check` on every push via GitHub Actions.

## License

[MIT](LICENSE)
