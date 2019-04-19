use futures::future::{self, Future};
use redis::{
    cmd, r#async::ConnectionLike, Cmd, ErrorKind, FromRedisValue, RedisFuture, ToRedisArgs,
};

impl<T> Commands for T where T: ConnectionLike + Sized + Send + 'static {}

pub trait Commands: Sized + ConnectionLike + Send + 'static {
    fn query<T>(self, cmd: &Cmd) -> RedisFuture<(Self, T)>
    where
        T: FromRedisValue + Send + 'static,
    {
        cmd.query_async(self)
    }

    fn execute<T>(self, cmd: &Cmd) -> RedisFuture<(Self, ())>
    where
        T: FromRedisValue + Send + 'static,
    {
        cmd.query_async(self)
    }

    fn ping(self) -> RedisFuture<(Self, ())> {
        Box::new(
            Cmd::new()
                .arg("PING")
                .query_async::<_, String>(self)
                .and_then(|(conn, rsp)| {
                    if rsp == "PONG" {
                        Ok((conn, ()))
                    } else {
                        Err((ErrorKind::IoError, "ping failed").into())
                    }
                }),
        )
    }

    /// Gets all keys matching pattern
    fn keys<K: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        key: K,
    ) -> RedisFuture<(Self, RV)> {
        cmd("KEYS").arg(key).query_async(self)
    }

    /// Get the value of a key.  If key is a vec this becomes an `MGET`.
    fn get<K: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        key: K,
    ) -> RedisFuture<(Self, RV)> {
        cmd(if key.is_single_arg() { "GET" } else { "MGET" })
            .arg(key)
            .query_async(self)
    }

    /// Set the string value of a key.
    fn set<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        key: K,
        value: V,
    ) -> RedisFuture<(Self, RV)> {
        cmd("SET").arg(key).arg(value).query_async(self)
    }

    /// Sets multiple keys to their values.
    fn set_multiple<K: ToRedisArgs, V: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        items: &[(K, V)],
    ) -> RedisFuture<(Self, RV)> {
        cmd("MSET").arg(items).query_async(self)
    }

    /// Delete one or more keys.
    fn del<K: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        key: K,
    ) -> RedisFuture<(Self, RV)> {
        cmd("DEL").arg(key).query_async(self)
    }

    /// Determine if a key exists.
    fn exists<K: ToRedisArgs, RV: FromRedisValue + Send + 'static>(
        self,
        key: K,
    ) -> RedisFuture<(Self, RV)> {
        cmd("EXISTS").arg(key).query_async(self)
    }
}
