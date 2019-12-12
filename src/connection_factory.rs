use std::error::Error as StdError;
use std::fmt;
use std::time::Instant;

use futures::{
    future::{self, Future},
    Poll,
};

use crate::{Ping, Poolable};

/// A factory for connections that always creates
/// connections to the same node.
///
/// The factory also performs pings on a node by using a fresh connection on each
/// ping.
pub trait ConnectionFactory {
    type Connection: Poolable;
    /// Create a new connection
    fn create_connection(&self) -> NewConnection<Self::Connection>;
    /// The node this factory will connect to when a new
    /// connection is requested.
    fn connecting_to(&self) -> &str;
    /// Ping the connection
    ///
    /// This will create a new connection and try a ping on it.
    ///
    /// If a factory does not support `ping` it will simply fail with `()`.
    fn ping(&self, _timeout: Instant) -> Box<dyn Future<Item = Ping, Error = ()> + Send> {
        Box::new(future::err(()))
    }
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

impl From<Box<dyn StdError + Send + Sync + 'static>> for NewConnectionError {
    fn from(cause: Box<dyn StdError + Send + Sync + 'static>) -> Self {
        Self { cause }
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

pub struct NewConnection<T: Poolable> {
    inner: Box<dyn Future<Item = T, Error = NewConnectionError> + Send + 'static>,
}

impl<T: Poolable> NewConnection<T> {
    pub fn new<F>(f: F) -> Self
    where
        F: Future<Item = T, Error = NewConnectionError> + Send + 'static,
    {
        Self { inner: Box::new(f) }
    }
}

impl<T: Poolable> Future for NewConnection<T> {
    type Item = T;
    type Error = NewConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}
