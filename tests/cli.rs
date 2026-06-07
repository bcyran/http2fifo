use std::{
    path::PathBuf,
    process::{Command, Stdio},
    time::Duration,
};

use tempfile::TempDir;
use tokio::process::Command as TokioCommand;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_http2fifo")
}

fn spawn_reader(path: PathBuf) -> tokio::task::JoinHandle<Vec<u8>> {
    tokio::spawn(async move {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        tokio::task::spawn_blocking(move || {
            use std::io::Read as _;
            let mut file = std::fs::File::open(&path).expect("reader: open failed");
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).expect("reader: read failed");
            buf
        })
        .await
        .expect("reader task panicked")
    })
}

#[test]
fn help_exits_zero() {
    let output = Command::new(binary_path())
        .arg("--help")
        .output()
        .expect("failed to run --help");

    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("Usage: http2fifo"));
}

#[test]
fn rejects_mixed_positional_and_mount_forms() {
    let output = Command::new(binary_path())
        .args([
            "http://example.test",
            "/tmp/a.fifo",
            "--mount",
            "http://example.test",
            "/tmp/b.fifo",
        ])
        .output()
        .expect("failed to run mixed-form CLI invocation");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot combine positional <URL>/<FIFO_PATH> with --mount flags"));
}

#[test]
fn rejects_malformed_header() {
    let output = Command::new(binary_path())
        .args([
            "--mount",
            "http://example.test",
            "/tmp/a.fifo",
            "-H",
            "broken",
        ])
        .output()
        .expect("failed to run malformed-header CLI invocation");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("header must be NAME:VALUE"));
}

#[tokio::test]
async fn http_error_exits_one_and_reports_mount_path() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&server)
        .await;

    let dir = TempDir::new().expect("failed to create tempdir");
    let fifo_path = dir.path().join("stream.fifo");
    let fifo_arg = fifo_path.to_string_lossy().into_owned();

    let reader = spawn_reader(fifo_path.clone());

    let child = TokioCommand::new(binary_path())
        .arg(server.uri())
        .arg(&fifo_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output();

    let output = tokio::time::timeout(Duration::from_secs(5), child)
        .await
        .expect("binary did not exit in time")
        .expect("failed to run CLI binary");

    let _ = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader task timed out")
        .expect("reader join failed");

    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(&format!("error: {}:", fifo_path.display())));
    assert!(!fifo_path.exists(), "FIFO should be unlinked on failure");
}

#[tokio::test]
async fn happy_path_streams_payload() {
    let payload: &[u8] = b"hello from cli";

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
        .mount(&server)
        .await;

    let dir = TempDir::new().expect("failed to create tempdir");
    let fifo_path = dir.path().join("stream.fifo");
    let fifo_arg = fifo_path.to_string_lossy().into_owned();

    let reader = spawn_reader(fifo_path.clone());

    let mut child = TokioCommand::new(binary_path())
        .arg(server.uri())
        .arg(&fifo_arg)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn CLI binary");

    let bytes = tokio::time::timeout(Duration::from_secs(5), reader)
        .await
        .expect("reader task timed out")
        .expect("reader join failed");
    assert_eq!(bytes, payload);

    let status = child.try_wait().expect("failed to query child status");
    assert!(
        status.is_none(),
        "binary should keep running for next reconnect"
    );

    child.start_kill().expect("failed to terminate CLI binary");
    let _ = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output())
        .await
        .expect("terminated binary did not exit in time")
        .expect("failed to collect CLI output");
}
