use std::fmt;
use std::time::Duration;

use crate::{Immediately, Wait};

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
