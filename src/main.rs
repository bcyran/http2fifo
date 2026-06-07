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

/// Logging verbosity level for `--log-level`.
#[derive(Clone, clap::ValueEnum)]
enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    const fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

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

    /// Logging verbosity; overrides `RUST_LOG` when set
    #[arg(long, value_name = "LEVEL")]
    log_level: Option<LogLevel>,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    init_tracing(cli.log_level);

    let fail_fast = cli.fail_fast;
    let method = parse_method(&cli.method);
    let headers: Vec<(String, String)> = cli.headers.iter().map(|h| parse_header(h)).collect();
    let body = cli.data.map(|s| bytes::Bytes::from(s.into_bytes()));
    let configs = build_configs(cli.url, cli.fifo_path, &cli.mounts, method, headers, body);

    let cancel = CancellationToken::new();
    let (got_sigint, got_sigterm) = spawn_signal_handler(&cancel);

    let results = mount_all(configs, cancel, fail_fast).await;
    report_errors(&results);

    std::process::exit(compute_exit_code(
        got_sigint.load(Ordering::Relaxed),
        got_sigterm.load(Ordering::Relaxed),
        &results,
    ));
}

/// Install the `tracing-subscriber`. If `log_level` is given it takes
/// precedence over `RUST_LOG`; otherwise `RUST_LOG` is used (and if that is
/// also unset, logging is silent).
fn init_tracing(log_level: Option<LogLevel>) {
    let filter = log_level.map_or_else(tracing_subscriber::EnvFilter::from_default_env, |l| {
        tracing_subscriber::EnvFilter::new(l.as_str())
    });
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

/// Parse an HTTP method string. Exits with code 1 on invalid input.
fn parse_method(s: &str) -> reqwest::Method {
    match s.parse::<reqwest::Method>() {
        Ok(m) => m,
        Err(e) => {
            eprintln!("error: invalid HTTP method '{s}': {e}");
            std::process::exit(1);
        }
    }
}

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

/// Validate the CLI mount arguments and build a list of [`Config`] values.
/// Exits with code 1 on invalid combinations.
fn build_configs(
    url: Option<String>,
    fifo_path: Option<PathBuf>,
    mounts: &[String],
    method: reqwest::Method,
    headers: Vec<(String, String)>,
    body: Option<bytes::Bytes>,
) -> Vec<Config> {
    let has_positionals = url.is_some() || fifo_path.is_some();
    let has_mounts = !mounts.is_empty();

    if has_positionals && has_mounts {
        eprintln!("error: cannot combine positional <URL>/<FIFO_PATH> with --mount flags");
        std::process::exit(1);
    } else if has_positionals {
        if let (Some(url), Some(fifo_path)) = (url, fifo_path) {
            vec![make_config(url, fifo_path, method, headers, body)]
        } else {
            eprintln!("error: single-mount form requires both <URL> and <FIFO_PATH>");
            std::process::exit(1);
        }
    } else if has_mounts {
        mounts
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

/// Spawn signal handlers for SIGINT and SIGTERM. Cancels `cancel` when either
/// signal is received and returns flags indicating which signal fired.
fn spawn_signal_handler(cancel: &CancellationToken) -> (Arc<AtomicBool>, Arc<AtomicBool>) {
    let got_sigint = Arc::new(AtomicBool::new(false));
    let got_sigterm = Arc::new(AtomicBool::new(false));

    let token = cancel.clone();
    let si = got_sigint.clone();
    let st = got_sigterm.clone();
    tokio::spawn(async move {
        let mut sigint = signal(SignalKind::interrupt()).expect("failed to install SIGINT handler");
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = sigint.recv()  => si.store(true, Ordering::Relaxed),
            _ = sigterm.recv() => st.store(true, Ordering::Relaxed),
        }
        token.cancel();
    });

    (got_sigint, got_sigterm)
}

/// Print errors for any failed mounts, suppressing [`Error::Cancelled`]
/// (which represents a clean shutdown).
fn report_errors(results: &[(PathBuf, Result<(), Error>)]) {
    for (path, result) in results {
        if let Err(e) = result
            && !matches!(e, Error::Cancelled)
        {
            eprintln!("error: {}: {e}", path.display());
        }
    }
}

/// Compute the process exit code from signal flags and mount results.
///
/// - `130` — interrupted by SIGINT
/// - `143` — terminated by SIGTERM
/// - `1`   — at least one mount failed
/// - `0`   — all mounts succeeded
fn compute_exit_code(
    got_sigint: bool,
    got_sigterm: bool,
    results: &[(PathBuf, Result<(), Error>)],
) -> i32 {
    if got_sigint {
        130
    } else if got_sigterm {
        143
    } else {
        i32::from(results.iter().any(|(_, r)| r.is_err()))
    }
}
