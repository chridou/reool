use std::sync::Arc;

use futures::future::{self, Future};
use redis::{aio::ConnectionLike, ErrorKind, RedisFuture, Value};

use crate::pools::pool_internal::Managed;
use crate::Poolable;

/// A connection that has been taken from the pool.
///
/// The connection returns when dropped unless there was an error.
///
/// Pooled connection implements `redis::async::ConnectionLike`
/// to easily integrate with code that already uses `redis-rs`.
pub struct PoolConnection<T: Poolable = ConnectionFlavour> {
    /// Track whether the connection is still in a valid state.
    ///
    /// If a future gets cancelled it is likely that the connection
    /// is not in a valid state anymore. For stateless connections this
    /// field is useless.
    pub(crate) connection_state_ok: bool,
    pub(crate) managed: Managed<T>,
}

impl<T: Poolable> PoolConnection<T> {
    pub fn connected_to(&self) -> &str {
        self.managed.connected_to()
    }
}

impl<T: Poolable> ConnectionLike for PoolConnection<T>
where
    T: ConnectionLike,
{
    fn req_packed_command(mut self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        if let Some(conn) = self.managed.value.take() {
            self.connection_state_ok = false;
            Box::new(conn.req_packed_command(cmd).map(|(conn, value)| {
                self.managed.value = Some(conn);
                self.connection_state_ok = true;
                (self, value)
            }))
        } else {
            Box::new(future::err(
                (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
            ))
        }
    }

    fn req_packed_commands(
        mut self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        if let Some(conn) = self.managed.value.take() {
            self.connection_state_ok = false;
            Box::new(
                conn.req_packed_commands(cmd, offset, count)
                    .map(|(conn, values)| {
                        self.managed.value = Some(conn);
                        self.connection_state_ok = true;
                        (self, values)
                    }),
            )
        } else {
            Box::new(future::err(
                (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
            ))
        }
    }

    fn get_db(&self) -> i64 {
        if let Some(conn) = self.managed.value.as_ref() {
            conn.get_db()
        } else {
            -1
        }
    }
}

impl<T: Poolable> Drop for PoolConnection<T> {
    fn drop(&mut self) {
        if !self.connection_state_ok {
            self.managed.value.take();
        }
    }
}

pub enum ConnectionFlavour {
    RedisRs(redis::aio::Connection, Arc<String>),
    // Tls(?)
}

impl Poolable for ConnectionFlavour {
    fn connected_to(&self) -> &str {
        match self {
            ConnectionFlavour::RedisRs(_, c) => &c,
        }
    }
}

impl ConnectionLike for ConnectionFlavour {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        match self {
            ConnectionFlavour::RedisRs(conn, c) => Box::new(
                conn.req_packed_command(cmd)
                    .map(|(conn, v)| (ConnectionFlavour::RedisRs(conn, c), v)),
            ),
        }
    }

    fn req_packed_commands(
        self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        match self {
            ConnectionFlavour::RedisRs(conn, c) => Box::new(
                conn.req_packed_commands(cmd, offset, count)
                    .map(|(conn, v)| (ConnectionFlavour::RedisRs(conn, c), v)),
            ),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionFlavour::RedisRs(ref conn, _) => conn.get_db(),
        }
    }
}

impl<T: Poolable> ConnectionLike for Managed<T>
where
    T: ConnectionLike,
{
    fn req_packed_command(mut self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        if let Some(conn) = self.value.take() {
            Box::new(conn.req_packed_command(cmd).map(|(conn, value)| {
                self.value = Some(conn);
                (self, value)
            }))
        } else {
            Box::new(future::err(
                (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
            ))
        }
    }

    fn req_packed_commands(
        mut self,
        cmd: Vec<u8>,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        if let Some(conn) = self.value.take() {
            Box::new(
                conn.req_packed_commands(cmd, offset, count)
                    .map(|(conn, values)| {
                        self.value = Some(conn);
                        (self, values)
                    }),
            )
        } else {
            Box::new(future::err(
                (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
            ))
        }
    }

    fn get_db(&self) -> i64 {
        if let Some(conn) = self.value.as_ref() {
            conn.get_db()
        } else {
            -1
        }
    }
}
