use crate::{pool_internal::Managed, Poolable};

/// A connection that has been taken from the pool.
///
/// The connection returns when dropped unless there was an error.
///
/// Pooled connection implements `redis::async::ConnectionLike`
/// to easily integrate with code that already uses `redis-rs`.
pub struct PooledConnection<T: Poolable + redis::r#async::ConnectionLike> {
    /// Track whether the is still in a valid state.
    ///
    /// If a future gets cancelled it is likely that the connection
    /// is not in a valid state anymore. For stateless connections this
    /// field is useless.
    pub(crate) connection_state_ok: bool,
    pub(crate) managed: Managed<T>,
}

impl<T: Poolable + redis::r#async::ConnectionLike> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if !self.connection_state_ok {
            self.managed.value.take();
        }
    }
}

impl<T: Poolable + redis::r#async::ConnectionLike> std::ops::Deref for PooledConnection<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.managed.value.as_ref().unwrap()
    }
}
