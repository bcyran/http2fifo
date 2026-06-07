use std::{path::PathBuf, time::Duration};

use http2fifo::config::Config;
use tokio::task::JoinHandle;

const fn make_config(url: String, fifo_path: PathBuf) -> Config {
    Config {
        url,
        method: reqwest::Method::GET,
        body: None,
        headers: vec![],
        fifo_path,
    }
}

/// Polls until `path` appears on disk, then opens the FIFO for reading in a
/// `spawn_blocking` thread and reads until EOF. Returns the collected bytes.
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

mod mount {
    use std::time::Duration;

    use super::{make_config, spawn_reader};
    use http2fifo::{error::Error, mount};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    /// The payload served by the mock is received byte-for-byte via the FIFO.
    ///
    /// `mount` loops indefinitely after each clean stream end, so it runs as a
    /// background task and is cancelled once the reader has received its EOF.
    #[tokio::test]
    async fn delivers_payload_to_fifo() {
        let payload: &[u8] = b"hello from http2fifo";

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let fifo_path = dir.path().join("stream.fifo");

        let reader = spawn_reader(fifo_path.clone());

        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let mount_handle = tokio::spawn(mount(make_config(server.uri(), fifo_path), cancel));

        // Reader gets EOF when the first stream ends and mount closes the write end.
        assert_eq!(reader.await.unwrap(), payload);

        // Cancel mount (it is now waiting for a second reader).
        token.cancel();
        assert!(
            matches!(mount_handle.await.unwrap(), Err(Error::Cancelled)),
            "expected mount to return Err(Cancelled) after token cancel"
        );
    }

    /// Cancelling the token mid-stream returns `Err(Cancelled)` and unlinks
    /// the FIFO.
    #[tokio::test]
    async fn cancelled_mid_stream_unlinks_fifo() {
        // 10 MB of zeros — cannot transfer in 100 ms through a FIFO pipe buffer.
        let body = vec![0u8; 10 * 1024 * 1024];

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let fifo_path = dir.path().join("stream.fifo");

        // A reader must be present so mount can open the write end.
        let _reader = spawn_reader(fifo_path.clone());

        let cancel = CancellationToken::new();
        let token = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            token.cancel();
        });

        let result = mount(make_config(server.uri(), fifo_path.clone()), cancel).await;

        assert!(
            matches!(result, Err(Error::Cancelled)),
            "expected Err(Cancelled), got {result:?}"
        );
        assert!(!fifo_path.exists(), "FIFO should be unlinked after mount");
    }
}

mod mount_all {
    use super::{make_config, spawn_reader};
    use http2fifo::{error::Error, mount_all};
    use tempfile::TempDir;
    use tokio_util::sync::CancellationToken;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{method, path},
    };

    /// Two mounts run concurrently; each FIFO delivers its own distinct
    /// payload correctly.
    ///
    /// `mount_all` loops indefinitely; it runs as a background task and is
    /// cancelled once both readers have consumed their first streams.
    #[tokio::test]
    async fn delivers_distinct_payloads() {
        let payload_a: &[u8] = b"stream-alpha";
        let payload_b: &[u8] = b"stream-beta";

        let server_a = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload_a))
            .mount(&server_a)
            .await;

        let server_b = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(payload_b))
            .mount(&server_b)
            .await;

        let dir = TempDir::new().unwrap();
        let path_a = dir.path().join("a.fifo");
        let path_b = dir.path().join("b.fifo");

        let reader_a = spawn_reader(path_a.clone());
        let reader_b = spawn_reader(path_b.clone());

        let configs = vec![
            make_config(server_a.uri(), path_a),
            make_config(server_b.uri(), path_b),
        ];

        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let mount_handle = tokio::spawn(mount_all(configs, cancel, false));

        // Both readers get EOF after their first HTTP streams end.
        assert_eq!(reader_a.await.unwrap(), payload_a);
        assert_eq!(reader_b.await.unwrap(), payload_b);

        // Cancel all mounts (they are waiting for second readers).
        token.cancel();
        let results = mount_handle.await.unwrap();
        for (path, result) in &results {
            assert!(
                matches!(result, Err(Error::Cancelled)),
                "expected Err(Cancelled) for mount {path:?}, got {result:?}"
            );
        }
    }

    /// With `fail_fast = true`, the first mount error cancels all remaining
    /// mounts.
    ///
    /// Mount A: server returns 500 → `Err(Http(…))`.
    /// Mount B: large body that cannot finish before cancellation →
    /// `Err(Cancelled)`.
    #[tokio::test]
    async fn fail_fast_cancels_peers_on_error() {
        let server_a = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server_a)
            .await;

        let body_b = vec![0u8; 10 * 1024 * 1024];
        let server_b = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body_b))
            .mount(&server_b)
            .await;

        let dir = TempDir::new().unwrap();
        let path_a = dir.path().join("a.fifo");
        let path_b = dir.path().join("b.fifo");

        // Readers allow both mounts to advance past the FIFO-wait stage.
        let _reader_a = spawn_reader(path_a.clone());
        let _reader_b = spawn_reader(path_b.clone());

        let configs = vec![
            make_config(server_a.uri(), path_a),
            make_config(server_b.uri(), path_b),
        ];

        let cancel = CancellationToken::new();
        let results = mount_all(configs, cancel, true).await;

        let result_a = &results[0].1;
        let result_b = &results[1].1;

        assert!(
            matches!(result_a, Err(Error::Http(_))),
            "expected Err(Http) for mount A, got {result_a:?}"
        );
        assert!(
            matches!(result_b, Err(Error::Cancelled)),
            "expected Err(Cancelled) for mount B, got {result_b:?}"
        );
    }
}
