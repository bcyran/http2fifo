use std::path::PathBuf;

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
    pub fifo_path: PathBuf,
}
