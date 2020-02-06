use futures::prelude::*;
use redis::{aio::ConnectionLike, cmd, Cmd, ErrorKind, FromRedisValue, RedisFuture, ToRedisArgs};

impl<T> Commands for T where T: ConnectionLike + Sized + Send + 'static {}

/// A helper trait to easily execute common
/// asynchronous Redis commands on a
/// `redis::aio::ConnectionLike`
pub trait Commands: Sized + ConnectionLike + Send + 'static {
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

    /// Send a ping command.
    #[allow(clippy::needless_lifetimes)]
    fn ping<'a>(&'a mut self) -> RedisFuture<'a, ()> {
        async move {
            let response = Cmd::new()
                .arg("PING")
                .query_async::<_, String>(self)
                .await?;

            if response == "PONG" {
                Ok(())
            } else {
                Err((ErrorKind::IoError, "ping failed").into())
            }
        }
        .boxed()
    }

    /// Gets all keys matching pattern
    fn keys<'a, K, RV> (&'a mut self, key: K) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("KEYS").arg(key).query_async(self).await
        }
        .boxed()
    }

 /// Get the value of a key.  If key is a vec this becomes an `MGET`.
    fn get<'a, K, RV>(&'a mut self, key: K) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd(if key.is_single_arg() { "GET" } else { "MGET" })
                .arg(key)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Set the string value of a key.
    fn set<'a, K, V, RV>(&'a mut self, key: K, value: V) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + 'a,
        V: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("SET")
                .arg(key)
                .arg(value)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Set the value of a key, only if the key does not exist
    fn set_nx<'a, K, V, RV>(&'a mut self, key: K, value: V) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + 'a,
        V: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("SETNX")
                .arg(key)
                .arg(value)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Sets multiple keys to their values.
    fn set_multiple<'a, K, V, RV>(&'a mut self, items: &'a [(K, V)]) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + Sync + 'a,
        V: ToRedisArgs + Send + Sync + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("MSET")
                .arg(items)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Sets multiple keys to their values failing if at least one already exists.
    fn set_multiple_nx<'a, K, V, RV>(&'a mut self, items: &'a [(K, V)]) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + Sync + 'a,
        V: ToRedisArgs + Send + Sync + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("MSETNX")
                .arg(items)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Delete one or more keys.
    fn del<'a, K, RV>(&'a mut self, key: K) -> RedisFuture<'a, RV>
    where
        K: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("DEL")
                .arg(key)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Determine if one or more keys exist.
    fn exists<'a, K, RV>(
        &'a mut self,
        key: K,
    ) -> RedisFuture<RV>
    where
        K: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("EXISTS").
                arg(key)
                .query_async(self)
                .await
        }
        .boxed()
    }

    /// Determine the number of keys.
    fn db_size<'a, K, RV>(
        &'a mut self,
        key: K,
    ) -> RedisFuture<RV>
    where
        K: ToRedisArgs + Send + 'a,
        RV: FromRedisValue + Send + 'a,
    {
        async move {
            cmd("DBSIZE").
                arg(key)
                .query_async(self)
                .await
        }
        .boxed()
    }
}
