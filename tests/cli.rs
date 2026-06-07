const fn binary_path() -> &'static str {
    env!("CARGO_BIN_EXE_http2fifo")
}

mod args {
    use std::process::Command;

    use super::binary_path;

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
    fn rejects_mixed_positional_and_mount() {
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
}

mod run {
    use std::{path::PathBuf, process::Stdio, time::Duration};

    use super::binary_path;
    use tempfile::TempDir;
    use tokio::process::Command as TokioCommand;
    use tokio::task::JoinHandle;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_bytes, header, method, path},
    };

    fn spawn_reader(path: PathBuf) -> JoinHandle<Vec<u8>> {
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

    /// Spawn the binary with `extra_args` followed by `server_uri` and a
    /// fresh temporary FIFO path, wait for the FIFO to be fully read, kill
    /// the binary, and return the bytes that were written to the FIFO.
    async fn run_and_collect(extra_args: &[&str], server_uri: &str) -> Vec<u8> {
        let dir = TempDir::new().expect("failed to create tempdir");
        let fifo_path = dir.path().join("stream.fifo");
        let fifo_arg = fifo_path.to_string_lossy().into_owned();

        let reader = spawn_reader(fifo_path.clone());

        let mut child = TokioCommand::new(binary_path())
            .args(extra_args)
            .arg(server_uri)
            .arg(&fifo_arg)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("failed to spawn CLI binary");

        let bytes = tokio::time::timeout(Duration::from_secs(5), reader)
            .await
            .expect("reader timed out")
            .expect("reader join failed");

        child.start_kill().ok();
        let _ = tokio::time::timeout(Duration::from_secs(5), child.wait_with_output()).await;

        bytes
    }

    /// The binary streams a payload byte-for-byte to the FIFO and then
    /// keeps running, waiting for the next reader.
    #[tokio::test]
    async fn streams_payload_to_fifo() {
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

    /// An HTTP error causes the binary to print the FIFO path to stderr and
    /// exit with code 1.
    #[tokio::test]
    async fn http_error_prints_path_and_exits_one() {
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

    /// `-X <METHOD>` is forwarded to the HTTP request. The mock only matches
    /// the specified method; a wrong method would yield 404 and fail the test.
    #[tokio::test]
    async fn custom_method_is_used() {
        let payload: &[u8] = b"post response";
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
            .mount(&server)
            .await;

        let bytes = run_and_collect(&["-X", "POST"], &server.uri()).await;
        assert_eq!(bytes, payload);
    }

    /// `-H name:value` is forwarded as a request header. The mock only matches
    /// when the header is present; a missing header would yield 404.
    #[tokio::test]
    async fn custom_header_is_forwarded() {
        let payload: &[u8] = b"header response";
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(header("x-token", "secret"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
            .mount(&server)
            .await;

        let bytes = run_and_collect(&["-H", "x-token: secret"], &server.uri()).await;
        assert_eq!(bytes, payload);
    }

    /// `-d <DATA>` is sent as the request body. The mock only matches the
    /// exact body bytes; a wrong or missing body would yield 404.
    #[tokio::test]
    async fn custom_body_is_sent() {
        let payload: &[u8] = b"body accepted";
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_bytes(b"my-data".to_vec()))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
            .mount(&server)
            .await;

        let bytes = run_and_collect(&["-X", "POST", "-d", "my-data"], &server.uri()).await;
        assert_eq!(bytes, payload);
    }
}
