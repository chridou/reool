use std::time::Duration;

use futures::{future::Future, try_ready, Async, Poll};
use redis::{r#async::Connection, Client, IntoConnectionInfo};

use crate::error::ReoolError;
use crate::pool::{
    Checkout as PoolCheckout, ConnectionFactory, NewConnFuture, NewConnectionError, Poolable,
};
use crate::pooled_connection::*;

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

pub trait RedisPool {
    fn checkout(&self) -> Checkout;
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
