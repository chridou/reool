use std::sync::Arc;

use futures::prelude::*;
use redis::{aio::ConnectionLike, ErrorKind, RedisFuture, Value, Cmd, Pipeline};

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
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let conn = match &mut self.managed.value {
                Some(conn) => conn,
                None => return Err((ErrorKind::IoError, "no connection - this is a bug of reool").into()),
            };

            self.connection_state_ok = false;

            let value = conn.req_packed_command(cmd).await?;

            self.connection_state_ok = true;

            Ok(value)
        }
        .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let conn = match &mut self.managed.value {
                Some(conn) => conn,
                None => return Err((ErrorKind::IoError, "no connection - this is a bug of reool").into()),
            };

            self.connection_state_ok = false;

            let values = conn.req_packed_commands(pipeline, offset, count).await?;

            self.connection_state_ok = true;

            Ok(values)
        }
        .boxed()
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
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        match self {
            ConnectionFlavour::RedisRs(conn, _uri) => conn.req_packed_command(cmd),
        }
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        match self {
            ConnectionFlavour::RedisRs(conn, c) => conn.req_packed_commands(pipeline, offset, count),
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
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let conn = match &mut self.value {
                Some(conn) => conn,
                None => return Err((ErrorKind::IoError, "no connection - this is a bug of reool").into()),
            };

            let value = conn.req_packed_command(cmd).await?;

            Ok(value)
        }
        .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        pipeline: &'a Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let conn = match &mut self.value {
                Some(conn) => conn,
                None => return Err((ErrorKind::IoError, "no connection - this is a bug of reool").into()),
            };

            let values = conn.req_packed_commands(pipeline, offset, count).await?;

            Ok(values)
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        if let Some(conn) = self.value.as_ref() {
            conn.get_db()
        } else {
            -1
        }
    }
}
