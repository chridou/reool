use std::error::Error as StdError;
use std::fmt;

/// Defines the strategy by which idle connections are taken from the pool.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ActivationOrder {
    /// First In - First Out
    ///
    /// Connections are taken in the same order they were
    /// added/returned to the pool
    FiFo,
    /// First In - First Out
    ///
    /// The connections that were added/returned last will
    /// be taken from
    /// the pool first
    LiFo,
}

impl Default for ActivationOrder {
    fn default() -> Self {
        ActivationOrder::FiFo
    }
}

impl fmt::Display for ActivationOrder {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ActivationOrder::FiFo => write!(f, "FiFo"),
            ActivationOrder::LiFo => write!(f, "LiFo"),
        }
    }
}

impl std::str::FromStr for ActivationOrder {
    type Err = ParseActivationOrderError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "fifo" => Ok(ActivationOrder::FiFo),
            "lifo" => Ok(ActivationOrder::LiFo),
            invalid => Err(ParseActivationOrderError(format!(
                "'{}' is not a valid ActivationOrder. Only 'FiFo' and 'LiFo' are allowed.",
                invalid
            ))),
        }
    }
}

#[derive(Debug)]
pub struct ParseActivationOrderError(String);

impl fmt::Display for ParseActivationOrderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse ActivationOrder. {}", self.0)
    }
}

impl StdError for ParseActivationOrderError {
    fn description(&self) -> &str {
        "parse activation order initialization failed"
    }

    fn cause(&self) -> Option<&dyn StdError> {
        None
    }
}
