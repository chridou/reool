//! # Reool
//!
//! Currently in early development.
//!
//! ## About
//!
//! Reool is a connection pool for Redis based on [redis-rs](https://crates.io/crates/redis).
//!
//! Currently `reool` is a fixed size connection pool.
//! `Reool` provides an interface for instrumentation.
//!
//!
//! You should also consider multiplexing instead of a pool based on your needs.
//!
//! The `PooledConnection` of `reool` implements the `ConnectionLike`
//! interface of [redis-rs](https://crates.io/crates/redis) for easier integration.
//!
//! For documentation visit [crates.io](https://crates.io/crates/reool).
//!
//! ## License
//!
//! Reool is distributed under the terms of both the MIT license and the
//! Apache License (Version 2.0).
//!
//! See LICENSE-APACHE and LICENSE-MIT for details.
//! License: Apache-2.0/MIT
use std::time::Duration;

use crate::pool_internal::CheckoutManaged;

use futures::{future::Future, try_ready, Async, Poll};

mod activation_order;
mod backoff_strategy;
mod commands;
pub mod connection_factory;
mod error;
pub(crate) mod executor_flavour;
pub(crate) mod helpers;
pub mod instrumentation;
pub mod multi_node_pool;
pub mod node_pool;
mod pool_internal;
mod pooled_connection;

pub use crate::error::{CheckoutError, CheckoutErrorKind};
pub use commands::*;
pub use pooled_connection::PooledConnection;

mod redis_rs;

pub trait Poolable: Send + Sized + 'static {}

/// A `Future` that represents a checkout.
///
/// A `Checkout` can fail for various reasons.
///
/// The most common ones are:
/// * There was a timeout on the checkout and it timed out
/// * The queue size was limited and the limit was reached
/// * There are simply no connections available
pub struct Checkout<T: Poolable + redis::r#async::ConnectionLike>(CheckoutManaged<T>);

impl<T: Poolable + redis::r#async::ConnectionLike> Future for Checkout<T> {
    type Item = PooledConnection<T>;
    type Error = CheckoutError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let managed = try_ready!(self.0.poll());
        Ok(Async::Ready(PooledConnection {
            managed,
            connection_state_ok: true,
        }))
    }
}

/// A trait that can be used as an interface for a connection pool.
pub trait RedisPool {
    type Connection: Poolable + redis::r#async::ConnectionLike;
    /// Checkout a new connection and if the request has to be enqueued
    /// use a timeout as defined by the implementor.
    fn check_out(&self) -> Checkout<Self::Connection>;
    /// Checkout a new connection and if the request has to be enqueued
    /// use the given timeout or wait indefinetly.
    fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout<Self::Connection>;
}

mod stats {
    /// Simple statistics on the internals of the pool.
    ///
    /// The values are not very accurate since they
    /// are only the minimum and maximum values
    /// observed during a configurable interval.
    #[derive(Debug, Clone)]
    pub struct PoolStats {
        /// The amount of connections
        pub pool_size: MinMax,
        /// The number of connections that are currently checked out
        pub in_flight: MinMax,
        /// The number of pending requests for connections
        pub reservations: MinMax,
        /// The number of idle connections which are available for
        /// immediate checkout
        pub idle: MinMax,
        /// The number of accessible nodes.
        ///
        /// Unless connected to multiple nodes this value will be 1.
        pub node_count: usize,
    }

    impl Default for PoolStats {
        fn default() -> Self {
            Self {
                pool_size: MinMax::default(),
                in_flight: MinMax::default(),
                reservations: MinMax::default(),
                idle: MinMax::default(),
                node_count: 0,
            }
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub struct MinMax<T = usize>(pub T, pub T);

    impl<T> MinMax<T>
    where
        T: Copy,
    {
        pub fn min(&self) -> T {
            self.0
        }
        pub fn max(&self) -> T {
            self.1
        }
    }

    impl<T> Default for MinMax<T>
    where
        T: Default,
    {
        fn default() -> Self {
            Self(T::default(), T::default())
        }
    }
}
