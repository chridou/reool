use std::error::Error as StdError;
use std::fmt;
use std::result::Result as StdResult;

use redis::{ErrorKind as RedisErrorKind, RedisError};

pub type ReoolResult<T> = StdResult<T, ReoolError>;
pub type InitializationResult<T> = StdResult<T, InitializationError>;

#[derive(Debug)]
pub struct ReoolError {
    kind: ErrorKind,
    cause: Option<Box<StdError + Send + Sync>>,
}

impl ReoolError {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self { kind, cause: None }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    pub(crate) fn with_cause<E: StdError + Send + Sync + 'static>(
        kind: ErrorKind,
        cause: E,
    ) -> Self {
        Self {
            kind,
            cause: Some(Box::new(cause)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    NoConnection,
    Timeout,
    QueueLimitReached,
    TaskExecution,
}

impl fmt::Display for ReoolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref cause) = self.cause {
            write!(f, "{}: {}", self.description(), cause)
        } else {
            f.write_str(self.description())
        }
    }
}

impl StdError for ReoolError {
    fn description(&self) -> &str {
        match self.kind {
            ErrorKind::NoConnection => "there are no connections available",
            ErrorKind::Timeout => "there was no connection available in time",
            ErrorKind::QueueLimitReached => "the queue limit has been reached",
            ErrorKind::TaskExecution => "task execution failed",
        }
    }

    fn cause(&self) -> Option<&StdError> {
        self.cause.as_ref().map(|cause| &**cause as &StdError)
    }
}

impl From<ReoolError> for RedisError {
    fn from(error: ReoolError) -> Self {
        (RedisErrorKind::IoError, "reool error", error.to_string()).into()
    }
}

#[derive(Debug)]
pub struct InitializationError {
    cause: Box<StdError + Send + Sync>,
}

impl InitializationError {
    pub fn new<E: StdError + Send + Sync + 'static>(cause: E) -> Self {
        Self {
            cause: Box::new(cause),
        }
    }
}

impl fmt::Display for InitializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}: {}", self.description(), self.cause)
    }
}

impl StdError for InitializationError {
    fn description(&self) -> &str {
        "initialization failed"
    }

    fn cause(&self) -> Option<&StdError> {
        Some(&*self.cause)
    }
}
