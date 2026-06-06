use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use clap::Parser;
use tokio::signal::unix::{SignalKind, signal};
use tokio_util::sync::CancellationToken;

use http2fifo::{config::Config, error::Error, mount_all};

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(
    name = "http2fifo",
    version,
    about = "Mount an HTTP streaming resource as a Unix named pipe (FIFO)"
)]
struct Cli {
    /// HTTP endpoint to stream (single-mount form)
    url: Option<String>,

    /// FIFO path to create (single-mount form)
    fifo_path: Option<PathBuf>,

    /// Mount a stream to a FIFO (repeatable; mutually exclusive with positionals)
    ///
    /// Usage: `--mount URL FIFO_PATH`
    #[arg(long = "mount", num_args = 2, value_names = ["URL", "FIFO_PATH"], action = clap::ArgAction::Append)]
    mounts: Vec<String>,

    /// HTTP method applied to all mounts [default: GET]
    #[arg(short = 'X', long, default_value = "GET")]
    method: String,

    /// Request header as NAME:VALUE, applied to all mounts (repeatable)
    #[arg(short = 'H', long = "header")]
    headers: Vec<String>,

    /// Request body (UTF-8), applied to all mounts
    #[arg(short = 'd', long)]
    data: Option<String>,

    /// Cancel all mounts if any one fails
    #[arg(long)]
    fail_fast: bool,
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Parse `NAME:VALUE` (or `NAME: VALUE`) into a `(String, String)` pair.
/// Exits with code 1 on malformed input.
fn parse_header(s: &str) -> (String, String) {
    if let Some((name, value)) = s.split_once(':') {
        (name.trim().to_owned(), value.trim().to_owned())
    } else {
        eprintln!("error: header must be NAME:VALUE, got {s:?}");
        std::process::exit(1);
    }
}

/// Build a [`Config`] applying the shared method / headers / body to a
/// specific URL + FIFO path pair.
const fn make_config(
    url: String,
    fifo_path: PathBuf,
    method: reqwest::Method,
    headers: Vec<(String, String)>,
    body: Option<bytes::Bytes>,
) -> Config {
    Config {
        url,
        method,
        headers,
        body,
        fifo_path,
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // --- Method -----------------------------------------------------------
    let method = match cli.method.parse::<reqwest::Method>() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("error: invalid HTTP method '{}': {e}", cli.method);
            std::process::exit(1);
        }
    };

    // --- Headers ----------------------------------------------------------
    let headers: Vec<(String, String)> = cli.headers.iter().map(|h| parse_header(h)).collect();

    // --- Body -------------------------------------------------------------
    let body = cli.data.map(|s| bytes::Bytes::from(s.into_bytes()));

    // --- Configs ----------------------------------------------------------
    let has_positionals = cli.url.is_some() || cli.fifo_path.is_some();
    let has_mounts = !cli.mounts.is_empty();

    let configs: Vec<Config> = if has_positionals && has_mounts {
        eprintln!("error: cannot combine positional <URL>/<FIFO_PATH> with --mount flags");
        std::process::exit(1);
    } else if has_positionals {
        if let (Some(url), Some(fifo_path)) = (cli.url, cli.fifo_path) {
            vec![make_config(url, fifo_path, method, headers, body)]
        } else {
            eprintln!("error: single-mount form requires both <URL> and <FIFO_PATH>");
            std::process::exit(1);
        }
    } else if has_mounts {
        cli.mounts
            .chunks(2)
            .map(|pair| {
                make_config(
                    pair[0].clone(),
                    PathBuf::from(&pair[1]),
                    method.clone(),
                    headers.clone(),
                    body.clone(),
                )
            })
            .collect()
    } else {
        eprintln!(
            "error: provide either <URL> <FIFO_PATH> or at least one --mount <URL> <FIFO_PATH>"
        );
        std::process::exit(1);
    };

    // --- Signal handling --------------------------------------------------
    let cancel = CancellationToken::new();
    let got_sigint = Arc::new(AtomicBool::new(false));
    let got_sigterm = Arc::new(AtomicBool::new(false));

    {
        let token = cancel.clone();
        let si = got_sigint.clone();
        let st = got_sigterm.clone();
        tokio::spawn(async move {
            let mut sigint =
                signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
            let mut sigterm =
                signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
            tokio::select! {
                _ = sigint.recv()  => si.store(true, Ordering::Relaxed),
                _ = sigterm.recv() => st.store(true, Ordering::Relaxed),
            }
            token.cancel();
        });
    }

    // --- Run --------------------------------------------------------------
    let results = mount_all(configs, cancel, cli.fail_fast).await;

    // --- Report errors (suppress Cancelled — those are clean shutdowns) ---
    for (path, result) in &results {
        if let Err(e) = result
            && !matches!(e, Error::Cancelled)
        {
            eprintln!("error: {}: {e}", path.display());
        }
    }

    // --- Exit code --------------------------------------------------------
    let exit_code = if got_sigint.load(Ordering::Relaxed) {
        130
    } else if got_sigterm.load(Ordering::Relaxed) {
        143
    } else {
        i32::from(results.iter().any(|(_, r)| r.is_err()))
    };

    std::process::exit(exit_code);
}
