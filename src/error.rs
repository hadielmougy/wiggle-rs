use std::fmt;

/// The crate's error type.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A gRPC call failed with a status (NOT_FOUND, INVALID_ARGUMENT, ...).
    #[error("rpc {method} failed: {status}")]
    Rpc {
        method: &'static str,
        #[source]
        status: tonic::Status,
    },
    /// The connection could not be set up.
    #[error("transport: {0}")]
    Transport(#[from] tonic::transport::Error),
    /// A workflow topology is invalid (raised by `GraphBuilder::build`).
    #[error("invalid workflow: {0}")]
    Flow(String),
    /// A worker's handlers do not match the registered graph (raised by `Worker::start`).
    #[error("workflow registration: {0}")]
    Registration(String),
    /// A value could not be encoded to / decoded from JSON.
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    /// The server refused a reported run; it wrote nothing. `status` is HTTP-style (404, 409, ...).
    #[error("report of task {task_id} refused ({status}): {message}")]
    ReportRefused {
        task_id: String,
        status: i32,
        message: String,
    },
    /// `Client::await_completion` ran out of time; carries the last observed status.
    #[error("instance {id} still {status} after {timeout:?}")]
    Timeout {
        id: String,
        status: String,
        timeout: std::time::Duration,
    },
    /// Any other client-side misuse.
    #[error("{0}")]
    Invalid(String),
}

impl Error {
    /// The gRPC status code, when this is an RPC failure.
    pub fn code(&self) -> Option<tonic::Code> {
        match self {
            Error::Rpc { status, .. } => Some(status.code()),
            _ => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// How a step handler fails. Any `std::error::Error` converts into a *retryable* `StepError` (so
/// `?` works in handlers); [`StepError::permanent`] fails the step without retrying -- a bad
/// request rather than a transient blip, the equivalent of Java's `PermanentActivityException`.
///
/// Like `anyhow::Error`, it deliberately does not implement `std::error::Error` itself, which is
/// what lets every error type convert into it.
pub struct StepError {
    message: String,
    retryable: bool,
}

impl StepError {
    /// A failure the engine retries per the step's retry policy.
    pub fn retryable(message: impl Into<String>) -> Self {
        StepError { message: message.into(), retryable: true }
    }

    /// A failure that skips the retry policy and fails the step right away.
    pub fn permanent(message: impl Into<String>) -> Self {
        StepError { message: message.into(), retryable: false }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_retryable(&self) -> bool {
        self.retryable
    }
}

impl<E: std::error::Error> From<E> for StepError {
    fn from(e: E) -> Self {
        StepError::retryable(e.to_string())
    }
}

impl fmt::Debug for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StepError")
            .field("message", &self.message)
            .field("retryable", &self.retryable)
            .finish()
    }
}

impl fmt::Display for StepError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
