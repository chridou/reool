use std::time::Duration;

use futures::{future::Future, try_ready, Async, Poll};
use redis::{r#async::Connection, Client, IntoConnectionInfo};

use crate::error::ReoolError;
use crate::pool::{
    Checkout as PoolCheckout, ConnectionFactory, NewConnFuture, NewConnectionError, Poolable,
};

mod backoff_strategy;
mod commands;
mod error;
pub(crate) mod executor_flavour;
mod node_pool;
mod pool;
mod pooled_connection;

pub use backoff_strategy::BackoffStrategy;
pub use commands::*;
pub use node_pool::*;
pub use pooled_connection::PooledConnection;

/// A `Future` that represents a checkout.
///
/// A `Checkout` can fail for various reasons.
///
/// The most common ones are:
/// * There was a timeout on the checkout and it timed out
/// * The queue size was limited and the limit was reached
/// * There are simply no connections available
pub struct Checkout(PoolCheckout<Connection>);

impl Future for Checkout {
    type Item = PooledConnection;
    type Error = ReoolError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let managed = try_ready!(self.0.poll());
        Ok(Async::Ready(PooledConnection {
            managed,
            last_op_completed: true,
        }))
    }
}

/// A trait that can be used as an interface for a Redis pool.
pub trait RedisPool {
    /// Checkout a new connection and if the request has to be enqueued
    /// use a timeout as defined by the implementor.
    fn checkout(&self) -> Checkout;
    /// Checkout a new connection and if the request has to be enqueued
    /// use the given timeout or wait indefinetly.
    fn checkout_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout;
}

impl Poolable for Connection {}

impl ConnectionFactory for Client {
    type Connection = Connection;

    fn create_connection(&self) -> NewConnFuture<Self::Connection> {
        NewConnFuture::new(
            self.get_async_connection()
                .map_err(|err| NewConnectionError::new(err)),
        )
    }
}
