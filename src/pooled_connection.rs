use futures::future::{self, Future};
use redis::{aio::ConnectionLike, ErrorKind, RedisFuture, Value};

use crate::pool_internal::Managed;
use crate::Poolable;

/// A connection that has been taken from the pool.
///
/// The connection returns when dropped unless there was an error.
///
/// Pooled connection implements `redis::async::ConnectionLike`
/// to easily integrate with code that already uses `redis-rs`.
pub struct RedisConnection {
    /// Track whether the connection is still in a valid state.
    ///
    /// If a future gets cancelled it is likely that the connection
    /// is not in a valid state anymore. For stateless connections this
    /// field is useless.
    pub(crate) connection_state_ok: bool,
    pub(crate) managed: Managed<ConnectionFlavour>,
}

impl ConnectionLike for RedisConnection {
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

impl Drop for RedisConnection {
    fn drop(&mut self) {
        if !self.connection_state_ok {
            self.managed.value.take();
        }
    }
}

pub enum ConnectionFlavour {
    RedisRs(redis::aio::Connection),
    // Tls(?)
}

impl Poolable for ConnectionFlavour {}

impl ConnectionLike for ConnectionFlavour {
    fn req_packed_command(self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        match self {
            ConnectionFlavour::RedisRs(conn) => Box::new(
                conn.req_packed_command(cmd)
                    .map(|(conn, v)| (ConnectionFlavour::RedisRs(conn), v)),
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
            ConnectionFlavour::RedisRs(conn) => Box::new(
                conn.req_packed_commands(cmd, offset, count)
                    .map(|(conn, v)| (ConnectionFlavour::RedisRs(conn), v)),
            ),
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionFlavour::RedisRs(ref conn) => conn.get_db(),
        }
    }
}
