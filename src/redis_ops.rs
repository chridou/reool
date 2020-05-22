use std::time::{Duration, Instant};

use futures::prelude::*;
use redis::{aio::ConnectionLike, cmd, Cmd, ErrorKind, FromRedisValue, RedisFuture};

impl<T> RedisOps for T where T: ConnectionLike + Sized + Send + 'static {}

/// A helper trait to easily execute common
/// asynchronous Redis operations on a
/// `redis::aio::ConnectionLike`
pub trait RedisOps: Sized + ConnectionLike + Send + 'static {
    /// Execute a command and expect a result
    fn query<'a, T>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, T>
    where
        T: FromRedisValue + Send + 'static,
    {
        cmd.query_async(self).boxed()
    }

    /// Execute a command and do not expect a result and instead
    /// just check whether the command did not fail
    fn execute<'a, T>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, ()>
    where
        T: FromRedisValue + Send + 'static,
    {
        cmd.query_async(self).boxed()
    }

    /// Send a ping command and return the roundrip time if successful
    #[allow(clippy::needless_lifetimes)]
    fn ping<'a>(&'a mut self) -> RedisFuture<'a, Duration> {
        let started = Instant::now();
        async move {
            let response = Cmd::new()
                .arg("PING")
                .query_async::<_, String>(self)
                .await?;

            if response == "PONG" {
                Ok(started.elapsed())
            } else {
                Err((ErrorKind::IoError, "ping failed").into())
            }
        }
        .boxed()
    }

    /// Delete all the keys of all the existing databases, not just the currently selected one.
    /// This command never fails.
    ///
    /// The time-complexity for this operation is O(N), N being the number of keys in all existing databases.
    fn flushall<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move { cmd("FLUSHALL").query_async(self).await }.boxed()
    }

    /// Delete all the keys of all the existing databases
    ///
    /// Redis (since 4.0,0) is now able to delete keys in the background in a different thread
    /// without blocking the server. An ASYNC option was added to FLUSHALL and FLUSHDB
    /// in order to let the entire dataset or a single database to be freed asynchronously.
    ///
    /// Asynchronous FLUSHALL and FLUSHDB commands only delete keys that were present at the time the command was invoked. Keys created during an asynchronous flush will be unaffected.
    fn flushall_async<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move { cmd("FLUSHALL ASYNC").query_async(self).await }.boxed()
    }

    /// Ask the server to close the connection.
    ///
    /// The connection is closed as soon as all pending replies have been written to the client.
    fn quit<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move {
            let response = Cmd::new().query_async::<_, String>(self).await?;
            if response == "OK" {
                Ok(())
            } else {
                Err((ErrorKind::IoError, "quit failed").into())
            }
        }
        .boxed()
    }

    /// Determine the number of keys.
    fn db_size<'a>(&'a mut self) -> RedisFuture<i64> {
        async move { cmd("DBSIZE").query_async(self).await }.boxed()
    }
}
