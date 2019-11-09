use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;
use std::sync::Arc;

use futures::future::BoxFuture;

use crate::Poolable;

pub type NewConnection<'a, C> = BoxFuture<'a, Result<C, NewConnectionError>>;

pub trait ConnectionFactory {
    type Connection: Poolable;
    fn create_connection(&self) -> NewConnection<'_, Self::Connection>;
    fn connecting_to(&self) -> Cow<[Arc<String>]>;
}

#[derive(Debug)]
pub struct NewConnectionError {
    cause: Box<dyn StdError + Send + 'static>,
}

impl NewConnectionError {
    pub fn new<E>(cause: E) -> Self
    where
        E: StdError + Send + 'static,
    {
        Self {
            cause: Box::new(cause),
        }
    }
}

impl fmt::Display for NewConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "could not create a new connection: {}", self.cause)
    }
}

impl StdError for NewConnectionError {
    fn description(&self) -> &str {
        "could not create a new connection"
    }

    fn cause(&self) -> Option<&dyn StdError> {
        Some(&*self.cause)
    }
}
