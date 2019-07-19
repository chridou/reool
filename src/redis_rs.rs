use futures::future::Future;
use redis::{aio::Connection, Client};

use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
use crate::error::{InitializationError, InitializationResult};
use crate::pooled_connection::ConnectionFlavour;
use crate::Poolable;

impl Poolable for Connection {}

pub struct RedisRsFactory(Client);

impl RedisRsFactory {
    pub fn new(connect_to: String) -> InitializationResult<Self> {
        Ok(Self(Client::open(&*connect_to).map_err(|err| {
            InitializationError::new(
                format!("Could not create a redis-rs client to {}", connect_to),
                Some(Box::new(err)),
            )
        })?))
    }
}

impl ConnectionFactory for RedisRsFactory {
    type Connection = ConnectionFlavour;

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        NewConnection::new(
            self.0
                .get_async_connection()
                .map(ConnectionFlavour::RedisRs)
                .map_err(NewConnectionError::new),
        )
    }
}
