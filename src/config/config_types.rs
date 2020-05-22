use std::fmt;
use std::time::Duration;

use crate::{Immediately, Millis, Seconds, Wait};

/// Various options on retrieving a connection
/// that can be applied if a user wants to use the pool defaults
/// for retrieving a connection.
///
/// The default is to wait for 30ms.
///
/// This struct only slightly differs from `CheckoutMode`: It lacks
/// the variant `PoolDefault` since that variant would make no sense
/// as this enum describes the default behaviour of the pool.
///
/// This struct has the same behaviour as `CheckoutMode` regarding its
/// `From` implementations.
///
/// The default is to wait for 30ms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultPoolCheckoutMode {
    /// Expect a connection to be returned immediately.
    /// If there is none available return an error immediately.
    Immediately,
    /// Wait until there is a connection.
    ///
    /// Using this can be risky as connections are returned
    /// when dropped. If the pool has no idle connections left while
    /// none are returned a deadlock might occur. It is always safe to use
    /// this mode if  only the `RedisPool` itself is used as a connection since
    /// it will immediately return the used connection after each operation.
    Wait,
    /// Wait for at most the given `Duration`.
    ///
    /// The amount of time waited will in the end not be really exact.
    WaitAtMost(Duration),
}

impl Default for DefaultPoolCheckoutMode {
    fn default() -> Self {
        DefaultPoolCheckoutMode::WaitAtMost(Duration::from_millis(30))
    }
}

impl std::str::FromStr for DefaultPoolCheckoutMode {
    type Err = ParseDefaultPoolCheckoutModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "wait" => Ok(DefaultPoolCheckoutMode::Wait),
            "immediately" => Ok(DefaultPoolCheckoutMode::Immediately),
            milliseconds => Ok(DefaultPoolCheckoutMode::WaitAtMost(Duration::from_millis(
                milliseconds
                    .parse::<u64>()
                    .map_err(|err| ParseDefaultPoolCheckoutModeError(err.to_string()))?,
            ))),
        }
    }
}

impl From<Immediately> for DefaultPoolCheckoutMode {
    fn from(_: Immediately) -> Self {
        DefaultPoolCheckoutMode::Immediately
    }
}

impl From<Wait> for DefaultPoolCheckoutMode {
    fn from(_: Wait) -> Self {
        DefaultPoolCheckoutMode::Wait
    }
}

impl From<Duration> for DefaultPoolCheckoutMode {
    fn from(d: Duration) -> Self {
        if d != Duration::from_secs(0) {
            DefaultPoolCheckoutMode::WaitAtMost(d)
        } else {
            DefaultPoolCheckoutMode::Immediately
        }
    }
}

impl From<Millis> for DefaultPoolCheckoutMode {
    fn from(d: Millis) -> Self {
        DefaultPoolCheckoutMode::WaitAtMost(d.into())
    }
}

impl From<Seconds> for DefaultPoolCheckoutMode {
    fn from(d: Seconds) -> Self {
        DefaultPoolCheckoutMode::WaitAtMost(d.into())
    }
}

#[derive(Debug)]
pub struct ParseDefaultPoolCheckoutModeError(String);

impl fmt::Display for ParseDefaultPoolCheckoutModeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Could not parse ParseDefaultPoolCheckoutMode: {}",
            self.0
        )
    }
}

impl std::error::Error for ParseDefaultPoolCheckoutModeError {
    fn description(&self) -> &str {
        "parse default pool checkout mode failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

/// A timeout for commands which is applied to all commands on all connections.
///
/// This timeout is a default and can be overridden in several places to
/// adjust the behaviour.
///
/// The default is to timeout after 1 minute.
///
/// ## FromStr
///
/// ```rust
/// # use std::time::Duration;
/// use reool::config::DefaultCommandTimeout;
///
/// let never: DefaultCommandTimeout = "never".parse().unwrap();
/// assert_eq!(never, DefaultCommandTimeout::Never);
///
/// let after100ms: DefaultCommandTimeout = "100".parse().unwrap();
/// assert_eq!(after100ms, DefaultCommandTimeout::After(Duration::from_millis(100)));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultCommandTimeout {
    /// Never time out.
    ///
    /// This requires a timeout to be applied from externally unless
    /// it is guaranted that a connection is killed eventually.
    Never,
    /// Wait for at most the given `Duration` until a
    /// Redis command has finished.
    ///
    /// If elapsed abort (drop) the connection.
    After(Duration),
}

impl Default for DefaultCommandTimeout {
    fn default() -> Self {
        DefaultCommandTimeout::After(Duration::from_secs(60))
    }
}

impl std::str::FromStr for DefaultCommandTimeout {
    type Err = ParseDefaultCommandTimeoutError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "never" => Ok(DefaultCommandTimeout::Never),
            milliseconds => Ok(DefaultCommandTimeout::After(Duration::from_millis(
                milliseconds
                    .parse::<u64>()
                    .map_err(|err| ParseDefaultCommandTimeoutError(err.to_string()))?,
            ))),
        }
    }
}

impl From<Wait> for DefaultCommandTimeout {
    fn from(_: Wait) -> Self {
        DefaultCommandTimeout::Never
    }
}

impl From<Millis> for DefaultCommandTimeout {
    fn from(d: Millis) -> Self {
        DefaultCommandTimeout::After(d.into())
    }
}

impl From<Seconds> for DefaultCommandTimeout {
    fn from(d: Seconds) -> Self {
        DefaultCommandTimeout::After(d.into())
    }
}

#[derive(Debug)]
pub struct ParseDefaultCommandTimeoutError(String);

impl fmt::Display for ParseDefaultCommandTimeoutError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Could not parse ParseDefaultCommandTimeoutError: {}",
            self.0
        )
    }
}

impl std::error::Error for ParseDefaultCommandTimeoutError {
    fn description(&self) -> &str {
        "parse default command timeout failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}
