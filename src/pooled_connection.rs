use futures::future::{self, Future};
use redis::{
    r#async::{Connection, ConnectionLike},
    ErrorKind, RedisFuture, Value,
};

use crate::pool::Managed;

pub struct PooledConnection {
    /// Track whether the future has not been cancelled while executing a query
    pub(crate) last_op_completed: bool,
    pub(crate) managed: Managed<Connection>,
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if !self.last_op_completed {
            self.managed.value.take();
        }
    }
}

impl ConnectionLike for PooledConnection {
    fn req_packed_command(mut self, cmd: Vec<u8>) -> RedisFuture<(Self, Value)> {
        if let Some(conn) = self.managed.value.take() {
            self.last_op_completed = false;
            Box::new(conn.req_packed_command(cmd).map(|(conn, value)| {
                self.managed.value = Some(conn);
                self.last_op_completed = true;
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
            self.last_op_completed = false;
            Box::new(
                conn.req_packed_commands(cmd, offset, count)
                    .map(|(conn, values)| {
                        self.managed.value = Some(conn);
                        self.last_op_completed = true;
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
