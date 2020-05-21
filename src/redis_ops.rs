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

    fn flushall<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move { cmd("FLUSHALL").query_async(self).await }.boxed()
    }

    fn flushall_async<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move { cmd("FLUSHALL ASYNC").query_async(self).await }.boxed()
    }

    /// Determine the number of keys.
    fn db_size<'a>(&'a mut self) -> RedisFuture<i64> {
        async move { cmd("DBSIZE").query_async(self).await }.boxed()
    }
}
