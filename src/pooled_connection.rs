use crate::{pool::Managed, Poolable};

/// A connection that has been taken from the pool.
///
/// The connection returns when dropped unless there was an error.
///
/// Pooled connection implements `redis::async::ConnectionLike`
/// to easily integrate with code that already uses `redis-rs`.
pub struct PooledConnection<T: Poolable + redis::r#async::ConnectionLike> {
    /// Track that the future has not been cancelled while executing a query
    pub(crate) last_op_completed: bool,
    pub(crate) managed: Managed<T>,
}

impl<T: Poolable + redis::r#async::ConnectionLike> Drop for PooledConnection<T> {
    fn drop(&mut self) {
        if !self.last_op_completed {
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
