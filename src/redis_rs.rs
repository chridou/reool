use futures::future::{self, Future};
use redis::{
    r#async::{Connection, ConnectionLike},
    Client, ErrorKind, RedisFuture, Value,
};

use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
use crate::pooled_connection::PooledConnection;
use crate::Poolable;

impl Poolable for Connection {}

impl ConnectionFactory for Client {
    type Connection = Connection;

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        NewConnection::new(self.get_async_connection().map_err(NewConnectionError::new))
    }
}

impl<T: Poolable + ConnectionLike> ConnectionLike for PooledConnection<T> {
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
