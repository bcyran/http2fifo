use futures_util::{Stream, StreamExt as _};

use crate::{
    config::Config,
    error::{Error, Result},
};

/// Initiates an HTTP request described by `config` and returns the response
/// body as a streaming sequence of [`bytes::Bytes`] chunks.
///
/// # Errors
///
/// - [`Error::Http`] — the request could not be sent (DNS, connection, TLS,
///   timeout), the server returned a non-2xx status, or a chunk could not be
///   read from the response body.
pub async fn fetch_stream(config: &Config) -> Result<impl Stream<Item = Result<bytes::Bytes>>> {
    tracing::debug!(method = %config.method, url = %config.url, "sending request");
    let client = reqwest::Client::new();

    let mut builder = client.request(config.method.clone(), &config.url);

    for (name, value) in &config.headers {
        builder = builder.header(name, value);
    }

    if let Some(body) = config.body.clone() {
        builder = builder.body(body);
    }

    let response = builder.send().await?;
    tracing::debug!(status = %response.status(), url = %config.url, "response received");
    let response = response.error_for_status()?;

    Ok(response.bytes_stream().map(|r| r.map_err(Error::Http)))
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures_util::StreamExt as _;
    use wiremock::matchers::{body_bytes, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::config::Config;

    /// Build a minimal GET config pointing at the mock server root.
    fn get_config(base_url: &str) -> Config {
        Config {
            url: base_url.to_owned(),
            method: reqwest::Method::GET,
            body: None,
            headers: vec![],
            fifo_path: std::path::PathBuf::new(),
        }
    }

    /// Collect all chunks from a stream into a single `Vec<u8>`.
    async fn collect(stream: impl Stream<Item = Result<Bytes>>) -> Vec<u8> {
        stream
            .map(|r| r.expect("chunk error"))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .flat_map(|b| b.to_vec())
            .collect()
    }

    #[tokio::test]
    async fn happy_path_streams_body() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"hello world"))
            .mount(&server)
            .await;

        let config = get_config(&server.uri());
        let stream = fetch_stream(&config).await.expect("fetch failed");

        assert_eq!(collect(stream).await, b"hello world");
    }

    #[tokio::test]
    async fn non_2xx_returns_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;

        let config = get_config(&server.uri());
        let result = fetch_stream(&config).await;

        assert!(
            matches!(result, Err(Error::Http(_))),
            "expected Err(Error::Http), got Ok(_)"
        );
    }

    #[tokio::test]
    async fn request_headers_are_forwarded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(header("x-custom", "value"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let mut config = get_config(&server.uri());
        config.headers = vec![("x-custom".to_owned(), "value".to_owned())];

        // If the header is missing wiremock returns 404, turning this into an error.
        let stream = fetch_stream(&config)
            .await
            .expect("request with custom header failed");
        drop(stream);
    }

    #[tokio::test]
    async fn request_body_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(body_bytes(b"payload".to_vec()))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let config = Config {
            url: server.uri(),
            method: reqwest::Method::POST,
            body: Some(Bytes::from_static(b"payload")),
            headers: vec![],
            fifo_path: std::path::PathBuf::new(),
        };

        let stream = fetch_stream(&config).await.expect("POST with body failed");
        drop(stream);
    }
}
