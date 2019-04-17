use std::error::Error as StdError;
use std::fmt;
use std::result::Result as StdResult;

pub type Result<T> = StdResult<T, Error>;

#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    cause: Option<Box<StdError + Send + Sync>>,
}

impl Error {
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
    TaskExecution,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(ref cause) = self.cause {
            write!(f, "{}: {}", self.description(), cause)
        } else {
            f.write_str(self.description())
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match self.kind {
            ErrorKind::NoConnection => "there are no connections available",
            ErrorKind::TaskExecution => "task execution failed",
        }
    }

    fn cause(&self) -> Option<&StdError> {
        self.cause.as_ref().map(|cause| &**cause as &StdError)
    }
}
