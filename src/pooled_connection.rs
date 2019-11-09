use std::sync::Arc;

use futures::FutureExt;
use redis::{aio::ConnectionLike, ErrorKind, RedisFuture, Value, Cmd, Pipeline};

use crate::pools::pool_internal::Managed;
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

impl RedisConnection {
    pub(crate) fn from_ok_managed(managed: Managed<ConnectionFlavour>) -> Self {
        Self {
            managed,
            connection_state_ok: true,
        }
    }

    pub fn connected_to(&self) -> &str {
        self.managed.connected_to()
    }
}

impl ConnectionLike for RedisConnection {
    fn req_packed_command(mut self, cmd: &Cmd) -> RedisFuture<(Self, Value)> {
        async {
            if let Some(conn) = self.managed.value.take() {
                self.connection_state_ok = false;

                let value = conn.req_packed_command(cmd).await?;

                self.managed.value = Some(conn);
                self.connection_state_ok = true;

                Ok((self, value))
            } else {
                Err((ErrorKind::IoError, "no connection - this is a bug of reool").into())
            }
        }.boxed()
    }

    fn req_packed_commands(
        mut self,
        cmds: &Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        async {
            if let Some(conn) = self.managed.value.take() {
                self.connection_state_ok = false;

                let values = conn.req_packed_commands(cmds, offset, count).await?;

                self.managed.value = Some(conn);
                self.connection_state_ok = true;

                Ok((self, values))
            } else {
                Err((ErrorKind::IoError, "no connection - this is a bug of reool").into())
            }
        }.boxed()
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
    fn req_packed_command(self, cmd: &Cmd) -> RedisFuture<(Self, Value)> {
        async {
            match self {
                ConnectionFlavour::RedisRs(conn, c) => {
                    let value = conn.req_packed_command(cmd).await?;
                    Ok((ConnectionFlavour::RedisRs(conn, c), value))
                },
            }
        }.boxed()
    }

    fn req_packed_commands(
        self,
        cmds: &Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<(Self, Vec<Value>)> {
        async {
            match self {
                ConnectionFlavour::RedisRs(conn, c) => {
                    let values = conn.req_packed_commands(cmds, offset, count).await?;
                    Ok((ConnectionFlavour::RedisRs(conn, c), values))
                },
            }
        }.boxed()
    }

    fn get_db(&self) -> i64 {
        match self {
            ConnectionFlavour::RedisRs(ref conn, _) => conn.get_db(),
        }
    }
}
