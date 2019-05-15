use std::error::Error as StdError;
use std::fmt;

use redis::{ErrorKind as RedisErrorKind, RedisError};

pub type ReoolResult<T> = Result<T, ReoolError>;
pub type InitializationResult<T> = Result<T, InitializationError>;

#[derive(Debug)]
pub struct ReoolError {
    kind: ErrorKind,
    cause: Option<Box<StdError + Send + Sync>>,
}

/// An error returned from `reool` when a checkout failed
impl ReoolError {
    pub(crate) fn new(kind: ErrorKind) -> Self {
        Self { kind, cause: None }
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

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Currently there is no connection available
    NoConnection,
    /// An enqueued request for a checkout that had a timeout
    /// set timed out
    CheckoutTimeout,
    /// No connection immediately available and no more
    /// requests can be enqueued to wait for a connection
    /// because the queue has reached it`s limit  
    QueueLimitReached,
    /// Something went wrong executing a task. Keep in
    /// mind that it depends on the `Executor` whether
    /// this error is returned. Some `Executor`s might simply
    /// panic.
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
            ErrorKind::CheckoutTimeout => "there was no connection to checkout available in time",
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

/// An initialization has failed
#[derive(Debug)]
pub struct InitializationError {
    message: Option<String>,
    cause: Option<Box<dyn StdError + Send + Sync>>,
}

impl InitializationError {
    pub fn new<T: Into<String>, E: StdError + Send + Sync + 'static>(
        msg: T,
        cause: Option<E>,
    ) -> Self {
        Self {
            message: Some(msg.into()),
            cause: cause.map(|cause| Box::new(cause) as Box<dyn StdError + Send + Sync>),
        }
    }

    pub fn message_only<T: Into<String>>(msg: T) -> Self {
        Self {
            message: Some(msg.into()),
            cause: None,
        }
    }

    pub fn cause_only<E: StdError + Send + Sync + 'static>(cause: E) -> Self {
        Self {
            message: None,
            cause: Some(Box::new(cause)),
        }
    }
}

impl fmt::Display for InitializationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (self.message.as_ref(), self.cause.as_ref()) {
            (Some(msg), Some(cause)) => write!(f, "{}: {}", msg, cause),
            (Some(msg), None) => write!(f, "{}", msg),
            (None, Some(cause)) => write!(f, "{}: {}", self.description(), cause),
            (None, None) => write!(f, "{}", self.description()),
        }
    }
}

impl StdError for InitializationError {
    fn description(&self) -> &str {
        "initialization failed"
    }

    fn cause(&self) -> Option<&dyn StdError> {
        self.cause.as_ref().map(|cause| &**cause as &dyn StdError)
    }
}
