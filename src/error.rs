use std::error::Error as StdError;
use std::fmt;

use redis::RedisError;

pub type InitializationResult<T> = Result<T, Error>;

/// An error specifying what went wrong
/// on a failed checkout
#[derive(Debug)]
pub struct CheckoutError {
    kind: CheckoutErrorKind,
}

/// An error returned from `reool` when a checkout failed
impl CheckoutError {
    pub(crate) fn new(kind: CheckoutErrorKind) -> Self {
        Self { kind }
    }

    /// The kind of the error which can be matched
    pub fn kind(&self) -> CheckoutErrorKind {
        self.kind
    }
}

/// Further specifies the kind of a `CheckoutError`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutErrorKind {
    /// Currently there is no connection available
    NoConnection,
    /// An enqueued request for a checkout that had a timeout
    /// set timed out. A reservation could not be fulfilled.
    CheckoutTimeout,
    /// No connection immediately available and no more
    /// requests can be enqueued to wait for a connection
    /// because the reservation queue has reached it`s limit
    ReservationLimitReached,
    /// No connection because there is no pool available.
    NoPool,
    /// If maximum number of checkout that can be
    /// enqueued has been reached
    CheckoutLimitReached,
    /// Something went wrong executing a task. Keep in
    /// mind that it depends on the `Executor` whether
    /// this error is returned. Some `Executor`s might simply
    /// panic.
    TaskExecution,
}

impl fmt::Display for CheckoutErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            CheckoutErrorKind::NoConnection => "there are no connections available",
            CheckoutErrorKind::CheckoutTimeout => {
                "there was no connection to checkout available in time"
            }
            CheckoutErrorKind::ReservationLimitReached => "the reservation limit has been reached",
            CheckoutErrorKind::NoPool => "there was no pool available",
            CheckoutErrorKind::CheckoutLimitReached => "checkout limit limit reached",
            CheckoutErrorKind::TaskExecution => "task execution failed",
        };
        f.write_str(s)
    }
}

impl fmt::Display for CheckoutError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.kind())?;
        Ok(())
    }
}

impl StdError for CheckoutError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        None
    }
}

impl From<CheckoutErrorKind> for CheckoutError {
    fn from(kind: CheckoutErrorKind) -> Self {
        Self { kind }
    }
}

impl From<CheckoutError> for RedisError {
    fn from(err: CheckoutError) -> Self {
        let err = std::io::Error::new(std::io::ErrorKind::NotConnected, err);
        err.into()
    }
}

impl From<redis::RedisError> for Error {
    fn from(err: redis::RedisError) -> Self {
        Self::new("redis error", Some(err))
    }
}

impl From<CheckoutError> for Error {
    fn from(err: CheckoutError) -> Self {
        Self::caused_by(err)
    }
}

/// An generic error
#[derive(Debug)]
pub struct Error {
    message: Option<String>,
    cause: Option<Box<dyn StdError + Send + Sync + 'static>>,
}

impl Error {
    pub fn new<T: Into<String>, E: StdError + Send + Sync + 'static>(
        msg: T,
        cause: Option<E>,
    ) -> Self {
        Self {
            message: Some(msg.into()),
            cause: cause.map(|cause| Box::new(cause) as Box<dyn StdError + Send + Sync + 'static>),
        }
    }

    pub fn message<T: Into<String>>(msg: T) -> Self {
        Self {
            message: Some(msg.into()),
            cause: None,
        }
    }

    pub fn caused_by<E: StdError + Send + Sync + 'static>(cause: E) -> Self {
        Self {
            message: None,
            cause: Some(Box::new(cause)),
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match (self.message.as_ref(), self.cause.as_ref()) {
            (Some(msg), Some(cause)) => write!(f, "{}: {}", msg, cause),
            (Some(msg), None) => write!(f, "{}", msg),
            (None, Some(cause)) => write!(f, "an error occurred: {}", cause),
            (None, None) => write!(f, "an error occurred"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.cause.as_ref().map(|cause| &**cause as &dyn StdError)
    }
}
