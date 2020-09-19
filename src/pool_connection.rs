use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use futures::prelude::*;
use redis::{aio::ConnectionLike, Cmd, ErrorKind, Pipeline, RedisError, RedisFuture, Value};
use tokio::time::timeout;

use crate::pools::pool_internal::Managed;
use crate::{config::DefaultCommandTimeout, Poolable};

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
    pub(crate) managed: Option<Managed<T>>,
    pub(crate) command_timeout: Option<Duration>,
}

impl<T: Poolable> PoolConnection<T> {
    /// Sets the timeout for each operation done with this connection.
    #[deprecated(
        since = "0.27.0",
        note = "Misleading name. Use 'set_timeout' or 'timeout'"
    )]
    pub fn default_command_timeout<TO: Into<DefaultCommandTimeout>>(&mut self, timeout: TO) {
        self.command_timeout = timeout.into().to_duration_opt();
    }

    /// Sets the timeout for each operation done with this connection.
    pub fn set_timeout<TO: Into<DefaultCommandTimeout>>(&mut self, timeout: TO) {
        self.command_timeout = timeout.into().to_duration_opt();
    }

    /// Sets the timeout for each operation done with this connection.
    pub fn timeout<TO: Into<DefaultCommandTimeout>>(mut self, timeout: TO) -> Self {
        self.set_timeout(timeout);
        self
    }

    fn get_connection(&mut self) -> Result<&mut T, IoError> {
        let managed = if let Some(managed) = &mut self.managed {
            managed
        } else {
            return Err(IoError::new(
                IoErrorKind::ConnectionAborted,
                "connection is broken due to a previous io error",
            ));
        };

        if let Some(connection) = managed.connection_mut() {
            Ok(connection)
        } else {
            Err(IoError::new(
                IoErrorKind::ConnectionAborted,
                "inner connection is invalid. THIS IS A BUG!",
            ))
        }
    }

    /// Invalidate the managed internal connection to prevent it from returning
    /// to the pool and also immediately drop the invalidated managed connection to
    /// trigger the creation of a new one
    fn invalidate(&mut self) {
        if let Some(mut managed) = self.managed.take() {
            managed.invalidate()
        }
        self.managed = None;
    }
}

impl<T: Poolable> ConnectionLike for PoolConnection<T>
where
    T: ConnectionLike,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            self.connection_state_ok = false;
            let command_timeout = self.command_timeout;

            let conn = self.get_connection()?;

            let f = conn.req_packed_command(cmd);
            let r = if let Some(command_timeout) = command_timeout {
                let started = Instant::now();
                match timeout(command_timeout, f).await {
                    Ok(r) => r,
                    Err(_) => {
                        let message = format!(
                            "command timeout after {:?} on `req_packed_command`.",
                            started.elapsed()
                        );
                        let err: RedisError =
                            (ErrorKind::IoError, "command timeout", message).into();
                        Err(err)
                    }
                }
            } else {
                f.await
            };

            match r {
                Ok(value) => {
                    self.connection_state_ok = true;

                    Ok(value)
                }
                Err(err) => {
                    match err.kind() {
                        // ErrorKind::ResponseError is a hack because the
                        // parsing files with 0 bytes and an unexpected EOF
                        // This behaviour need clarification.
                        // See https://github.com/mitsuhiko/redis-rs/issues/320
                        ErrorKind::IoError | ErrorKind::ResponseError => {
                            // TODO: Can we get a new connection?
                            self.invalidate();
                        }
                        _ => {
                            self.connection_state_ok = true;
                        }
                    }
                    Err(err)
                }
            }
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
            self.connection_state_ok = false;
            let command_timeout = self.command_timeout;

            let conn = self.get_connection()?;

            let f = conn.req_packed_commands(pipeline, offset, count);
            let r = if let Some(command_timeout) = command_timeout {
                let started = Instant::now();
                match timeout(command_timeout, f).await {
                    Ok(r) => r,
                    Err(_) => {
                        let message = format!(
                            "command timeout after {:?} on `req_packed_commands`.",
                            started.elapsed()
                        );
                        let err: RedisError =
                            (ErrorKind::IoError, "command timeout", message).into();
                        Err(err)
                    }
                }
            } else {
                f.await
            };

            match r {
                Ok(values) => {
                    self.connection_state_ok = true;

                    Ok(values)
                }
                Err(err) => {
                    match err.kind() {
                        // ErrorKind::ResponseError is a hack because the
                        // parsing files with 0 bytes and an unexpected EOF
                        // This behaviour need clarification.
                        // See https://github.com/mitsuhiko/redis-rs/issues/320
                        ErrorKind::IoError | ErrorKind::ResponseError => {
                            // TODO: Can we get a new connection?
                            self.invalidate();
                        }
                        _ => {
                            self.connection_state_ok = true;
                        }
                    }
                    Err(err)
                }
            }
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        if let Some(conn) = self.managed.as_ref() {
            conn.get_db()
        } else {
            -1
        }
    }
}

impl<T: Poolable> Drop for PoolConnection<T> {
    fn drop(&mut self) {
        if !self.connection_state_ok {
            self.invalidate();
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
            ConnectionFlavour::RedisRs(conn, _) => {
                conn.req_packed_commands(pipeline, offset, count)
            }
        }
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionFlavour::RedisRs(conn, _) => conn.get_db(),
        }
    }
}

impl<T: Poolable> ConnectionLike for Managed<T>
where
    T: ConnectionLike,
{
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let conn = match self.connection_mut() {
                Some(conn) => conn,
                None => {
                    return Err(
                        (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
                    )
                }
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
            let conn = match self.connection_mut() {
                Some(conn) => conn,
                None => {
                    return Err(
                        (ErrorKind::IoError, "no connection - this is a bug of reool").into(),
                    )
                }
            };

            let values = conn.req_packed_commands(pipeline, offset, count).await?;

            Ok(values)
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        if let Some(conn) = self.connection() {
            conn.get_db()
        } else {
            -1
        }
    }
}
