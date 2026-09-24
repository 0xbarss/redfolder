/// Result alias for operations in `redfolder`.
pub type Result<T> = std::result::Result<T, RedFolderError>;

/// Errors returned by the `redfolder` engine and calendar client.
#[derive(Debug, thiserror::Error)]
pub enum RedFolderError {
    #[error("HTTP request error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization / JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Failed to parse time string '{0}': expected format HH:MM (e.g. '20:30')")]
    ParseTime(String),

    #[error("Failed to parse date/timestamp: {0}")]
    InvalidTimestamp(String),

    #[error("Service error: {0}")]
    Service(String),

    #[error("Curfew calculation error: {0}")]
    Curfew(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Calendar error: {0}")]
    Calendar(String),
}
