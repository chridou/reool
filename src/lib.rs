//! # Reool
//!
//! ## About
//!
//! Reool is a REdis connection pOOL based on [redis-rs](https://crates.io/crates/redis).
//!
//! Reool is aimed at either connecting to a single primary node or
//! connecting to a replica set using the replicas as read only nodes.
//!
//! Currently Reool is a fixed size connection pool.
//! Reool provides an interface for instrumentation.
//!
//! You should also consider multiplexing instead of a pool based upon your needs.
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
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    try_ready, Async, Poll,
};

use crate::config::Builder;
use crate::pooled_connection::ConnectionFlavour;
use crate::pools::pool_internal::{CheckoutManaged, Managed};

pub mod config;
pub mod instrumentation;

pub use crate::error::{CheckoutError, CheckoutErrorKind};
pub use commands::Commands;
pub use pooled_connection::RedisConnection;

pub(crate) mod connection_factory;
pub(crate) mod executor_flavour;
pub(crate) mod helpers;

mod activation_order;
mod backoff_strategy;
mod commands;
mod error;
mod pooled_connection;
mod pools;
mod redis_rs;

pub trait Poolable: Send + Sized + 'static {
    fn connected_to(&self) -> &str;
}

/// A `Future` that represents a checkout.
///
/// A `Checkout` can fail for various reasons.
///
/// The most common ones are:
/// * There was a timeout on the checkout and it timed out
/// * The queue size was limited and the limit was reached
/// * There are simply no connections available
/// * There is no connected node
pub struct Checkout(CheckoutManaged<ConnectionFlavour>);

impl Checkout {
    pub(crate) fn new<F>(f: F) -> Self
    where
        F: Future<Item = Managed<ConnectionFlavour>, Error = CheckoutError> + Send + 'static,
    {
        Checkout(CheckoutManaged::new(f))
    }
}

impl Future for Checkout {
    type Item = RedisConnection;
    type Error = CheckoutError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let managed = try_ready!(self.0.poll());
        Ok(Async::Ready(RedisConnection {
            managed,
            connection_state_ok: true,
        }))
    }
}

/// Various options on retrieving a connection
///
/// ## `From` implementations
///
/// * `Duration`: `WaitAtMost` or `Immediately`
/// * `Option<Duration>>`: `PoolDefault` if `None` or
/// the mapping used for a `Duration` if `Some`
/// * `Instant`: `WaitAtMost` upd to the `Instant` if the instant is
/// in the future. Otherwise `Immediately`
/// * `Option<Instant>`: `PoolDefault` if `None` or
/// the mapping used for a `Instant` if `Some`
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutMode {
    /// Expect a connection to be returned immediately.
    /// If there is none available return an error immediately.
    Immediately,
    /// Use the default configured for the pool
    PoolDefault,
    /// Wait until there is a connection even if it would take forever.
    Wait,
    /// Wait for at most the given `Duration`.
    ///
    /// The amount of time waited will in the end not be really exact.
    WaitAtMost(Duration),
}

impl CheckoutMode {
    /// Do a sanity adjustment. E.g. it makes no sense to use
    /// `WaitAtMost(Duration::from_desc(0))` since this would logically be
    /// `Immediately`.
    pub fn adjust(self) -> Self {
        match self {
            CheckoutMode::WaitAtMost(d) if d == Duration::from_secs(0) => CheckoutMode::Immediately,
            x => x,
        }
    }
}

/// Simply a shortcut for `CheckkoutMode::Immediately`
#[derive(Debug, Clone, Copy)]
pub struct Immediately;

/// Simply a shortcut for `CheckkoutMode::Wait`
#[derive(Debug, Clone, Copy)]
pub struct Wait;

/// Simply a shortcut for `CheckkoutMode::PoolDefault`
#[derive(Debug, Clone, Copy)]
pub struct PoolDefault;

impl From<Immediately> for CheckoutMode {
    fn from(_: Immediately) -> Self {
        CheckoutMode::Immediately
    }
}

impl From<Wait> for CheckoutMode {
    fn from(_: Wait) -> Self {
        CheckoutMode::Wait
    }
}

impl From<PoolDefault> for CheckoutMode {
    fn from(_: PoolDefault) -> Self {
        CheckoutMode::PoolDefault
    }
}

impl Default for CheckoutMode {
    fn default() -> Self {
        CheckoutMode::PoolDefault
    }
}

impl From<Duration> for CheckoutMode {
    fn from(d: Duration) -> Self {
        if d != Duration::from_secs(0) {
            CheckoutMode::WaitAtMost(d)
        } else {
            CheckoutMode::Immediately
        }
    }
}

impl From<Option<Duration>> for CheckoutMode {
    fn from(d: Option<Duration>) -> Self {
        if let Some(d) = d {
            d.into()
        } else {
            Self::default()
        }
    }
}

impl From<Instant> for CheckoutMode {
    fn from(in_the_future: Instant) -> Self {
        if let Some(i) = in_the_future.checked_duration_since(Instant::now()) {
            i.into()
        } else {
            CheckoutMode::Immediately
        }
    }
}

impl From<Option<Instant>> for CheckoutMode {
    fn from(in_the_future: Option<Instant>) -> Self {
        if let Some(i) = in_the_future {
            i.into()
        } else {
            Self::default()
        }
    }
}

#[derive(Clone)]
enum RedisPoolFlavour {
    Empty,
    Shared(pools::SharedPool),
    PerNode(pools::PoolPerNode),
}

/// A pool to one or more Redis instances.
#[derive(Clone)]
pub struct RedisPool(RedisPoolFlavour);

impl RedisPool {
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub fn no_pool() -> Self {
        RedisPool(RedisPoolFlavour::Empty)
    }

    /// Checkout a new connection and if the request has to be enqueued
    /// use a timeout as defined by the pool as a default.
    pub fn check_out_pool_default(&self) -> Checkout {
        self.check_out(CheckoutMode::PoolDefault)
    }
    /// Checkout a new connection and choose whether to wait for a connection or not
    /// as defined by the `CheckoutMode`.
    pub fn check_out<M: Into<CheckoutMode>>(&self, mode: M) -> Checkout {
        match self.0 {
            RedisPoolFlavour::Shared(ref pool) => pool.check_out(mode),
            RedisPoolFlavour::PerNode(ref pool) => pool.check_out(mode),
            RedisPoolFlavour::Empty => Checkout(CheckoutManaged::new(future::err(
                CheckoutError::new(CheckoutErrorKind::NoPool),
            ))),
        }
    }

    /// Ping all the nodes which this pool is connected to.
    ///
    /// `timeout` is the maximum time allowed for a ping.
    pub fn ping(&self, timeout: Duration) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        match self.0 {
            RedisPoolFlavour::Shared(ref pool) => Box::new(pool.ping(timeout).map(|p| vec![p]))
                as Box<dyn Future<Item = _, Error = ()> + Send>,
            RedisPoolFlavour::PerNode(ref pool) => Box::new(pool.ping(timeout)),
            RedisPoolFlavour::Empty => Box::new(future::ok(vec![])),
        }
    }

    pub fn connected_to(&self) -> &[String] {
        match self.0 {
            RedisPoolFlavour::Shared(ref pool) => pool.connected_to(),
            RedisPoolFlavour::PerNode(ref pool) => pool.connected_to(),
            RedisPoolFlavour::Empty => &[],
        }
    }
}

#[derive(Debug)]
pub enum PingState {
    Ok,
    Failed(Box<dyn std::error::Error + Send>),
}

#[derive(Debug)]
pub struct Ping {
    pub latency: Duration,
    pub uri: Option<String>,
    pub state: PingState,
}

impl Ping {
    pub fn is_ok(&self) -> bool {
        match self.state {
            PingState::Ok => true,
            _ => false,
        }
    }

    pub fn is_failed(&self) -> bool {
        !self.is_ok()
    }
}
