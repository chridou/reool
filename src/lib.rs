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
//! ## The `ConnectionLike` trait
//!
//! Both `PooledConnection` and `RedisPool` implement the `ConnectionLike`
//! interface of [redis-rs](https://crates.io/crates/redis) for easy integration.
//!
//! `ConnectionLike::get_db` should be handled with care since the pool will always return
//! -1. Currently connections from the pool will also do so if the connection was teriminated
//! by an IO error. So `ConnectionLike::get_db` so always handled with care.
//!
//! ## Documentation
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

use futures::prelude::*;

use crate::config::Builder;
use crate::config::DefaultPoolCheckoutMode;

pub mod config;
pub mod instrumentation;

use future::BoxFuture;
pub use redis::{
    aio::ConnectionLike, cmd, AsyncCommands, Cmd, FromRedisValue, NumericBehavior, RedisError,
    RedisFuture, ToRedisArgs, Value,
};

pub use crate::error::{CheckoutError, CheckoutErrorKind};
pub use pool_connection::{ConnectionFlavour, PoolConnection};
pub use redis_ops::RedisOps;

pub mod connection_factory;
pub mod error;
pub(crate) mod executor_flavour;
pub(crate) mod helpers;

mod activation_order;
mod backoff_strategy;
mod pool_connection;
mod pools;
mod redis_ops;
mod redis_rs;

pub use redis;

/// Something that can be put into the connection pool
pub trait Poolable: Send + Sized + 'static {
    /// The host/addr this connection is connected to.
    fn connected_to(&self) -> &str;
}

/// Various options on retrieving a connection
///
/// ## Special `From` implementations
///
/// * `Duration`: `Until` with a deadline from now until the durations elapsed.
/// * `Instant`: `Until` the given instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckoutMode {
    /// Expect a connection to be returned immediately.
    /// If there is none available return an error immediately.
    /// In that case a `CheckoutErrorKind::NoConnection`
    /// will be returned
    ///
    /// This mode will always try to get a connection
    Immediately,
    /// Wait until there is a connection
    ///
    /// Using this can be risky as connections are returned
    /// when dropped. If the pool has no idle connections left while
    /// none are returned a deadlock might occur. It is always safe to use
    /// this mode if  only the `RedisPool` itself is used as a connection since
    /// it will immediately return the used connection after each operation.
    Wait,
    /// Use the default configured for the pool
    PoolDefault,
    /// Checkout before the given `Instant` is elapsed. If the given timeout is
    /// elapsed, no attempt to checkout a connection will be made.
    /// In that case a `CheckoutErrorKind::CheckoutTimeout` will be returned.
    Until(Instant),
}

impl CheckoutMode {
    pub fn is_deadline_elapsed(self) -> bool {
        match self {
            CheckoutMode::Until(deadline) => deadline < Instant::now(),
            _ => false,
        }
    }
}

/// Simply a shortcut for `CheckoutMode::Immediately`
#[derive(Debug, Clone, Copy)]
pub struct Immediately;

/// Simply a shortcut for `CheckoutMode::Wait`
///
/// Using this can be risky as connections are returned
/// when dropped. If the pool has no idle connections left while
/// none are returned a deadlock might occur. It is always safe to use
/// this mode if  only the `RedisPool` itself is used as a connection since
/// it will immediately return the used connection after each operation.
#[derive(Debug, Clone, Copy)]
pub struct Wait;

/// Simply a shortcut for `CheckoutMode::PoolDefault`
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
        let timeout = Instant::now() + d;
        timeout.into()
    }
}

impl From<Instant> for CheckoutMode {
    fn from(until: Instant) -> Self {
        CheckoutMode::Until(until)
    }
}

enum RedisPoolFlavour<T: Poolable> {
    Empty,
    Single(pools::SinglePool<T>),
    PerNode(pools::PoolPerNode<T>),
}

impl<T: Poolable> Clone for RedisPoolFlavour<T> {
    fn clone(&self) -> Self {
        use RedisPoolFlavour::*;
        match self {
            Empty => Empty,
            Single(pool) => Single(pool.clone()),
            PerNode(pool) => PerNode(pool.clone()),
        }
    }
}

/// A pool to one or more Redis instances.
///
/// This is the core type of this library.
///
/// ## Overview
///
/// Each `RedisPool` consist of one or more sub pools whereas each sub pool is only
/// ever connected to one Redis node.
///
/// So the number of sub pools is usually the number of nodes to connect to. Furthermore
/// the number of sub pools can be increased via the configuration.
///
/// `Reool` uses a stream to enqueue checkouts. Furthermore the number of buffered checkout
/// requests is always limited. Having multiple sub pools will increase the number of
/// checkout requests that can be enqueued.
///
/// When having more that one sub pool `Reool` will retry checkout attempts on different
/// sub pools.
///
/// ## `ConnectionLike`
///
/// The pool itself implements `ConnectionLike`. This is a convinience functionality
/// for executing a single redis command. The checkout will be done with the `DefaultCheckoutMode`
/// defined for the pool. Furthermore `ConnectionLike::get_db` will always return -1.
pub struct RedisPool<T: Poolable = ConnectionFlavour> {
    flavour: RedisPoolFlavour<T>,
    default_checkout_mode: DefaultPoolCheckoutMode,
    retry_on_checkout_limit: bool,
}

impl RedisPool {
    pub fn builder() -> Builder {
        Builder::default()
    }
}

impl<T: Poolable> RedisPool<T> {
    pub fn no_pool() -> Self {
        RedisPool {
            flavour: RedisPoolFlavour::Empty,
            default_checkout_mode: DefaultPoolCheckoutMode::Wait,
            retry_on_checkout_limit: false,
        }
    }

    /// Checkout a new connection and if the request has to be enqueued
    /// use a timeout as defined by the pool as a default.
    pub fn check_out_default<'a>(
        &'a self,
    ) -> impl Future<Output = Result<PoolConnection<T>, CheckoutError>> + 'a {
        self.check_out(CheckoutMode::PoolDefault)
    }

    /// Checkout a new connection and choose whether to wait for a connection or not
    /// as defined by the `CheckoutMode`.
    pub async fn check_out<M: Into<CheckoutMode>>(
        &self,
        mode: M,
    ) -> Result<PoolConnection<T>, CheckoutError> {
        let constraint = pools::CheckoutConstraint::from_checkout_mode_and_pool_default(
            mode,
            self.default_checkout_mode,
        );

        let managed = match self.flavour {
            RedisPoolFlavour::Single(ref pool) => {
                pools::check_out_maybe_retry_on_queue_limit_reached(
                    pool,
                    constraint,
                    self.retry_on_checkout_limit,
                )
                .await?
            }
            RedisPoolFlavour::PerNode(ref pool) => {
                pools::check_out_maybe_retry_on_queue_limit_reached(
                    pool,
                    constraint,
                    self.retry_on_checkout_limit,
                )
                .await?
            }
            RedisPoolFlavour::Empty => {
                let err = CheckoutError::new(CheckoutErrorKind::NoPool);
                return Err(err);
            }
        };

        Ok(PoolConnection {
            managed: Some(managed),
            connection_state_ok: true,
        })
    }

    pub fn connected_to(&self) -> Vec<String> {
        match self.flavour {
            RedisPoolFlavour::Single(ref pool) => vec![pool.connected_to().to_string()],
            RedisPoolFlavour::PerNode(ref pool) => pool.connected_to().to_vec(),
            RedisPoolFlavour::Empty => vec![],
        }
    }

    pub fn state(&self) -> PoolState {
        match self.flavour {
            RedisPoolFlavour::Single(ref pool) => pool.state(),
            RedisPoolFlavour::PerNode(ref pool) => pool.state(),
            RedisPoolFlavour::Empty => PoolState::default(),
        }
    }

    /// Ping all the nodes which this pool is connected to.
    ///
    /// `timeout` is the maximum time allowed for a ping.
    ///
    /// This method only fails with `()` if the underlying connection
    /// does not support pinging. All other errors will be contained
    /// in the returned `Ping` struct.
    pub fn ping_nodes<TO: Into<Timeout>>(&self, timeout: TO) -> BoxFuture<Vec<Ping>> {
        let deadline = timeout.into().0;

        match self.flavour {
            RedisPoolFlavour::Single(ref pool) => pool.ping(deadline).map(|p| vec![p]).boxed(),
            RedisPoolFlavour::PerNode(ref pool) => pool.ping(deadline).boxed(),
            RedisPoolFlavour::Empty => future::ready(vec![]).boxed(),
        }
    }
}

impl<T: Poolable> Clone for RedisPool<T> {
    fn clone(&self) -> Self {
        RedisPool {
            flavour: self.flavour.clone(),
            default_checkout_mode: self.default_checkout_mode,
            retry_on_checkout_limit: self.retry_on_checkout_limit,
        }
    }
}

/// A timeout which can also be seen as a deadline
pub struct Timeout(pub Instant);

impl From<Instant> for Timeout {
    fn from(at: Instant) -> Self {
        Self(at)
    }
}

impl From<Duration> for Timeout {
    fn from(d: Duration) -> Self {
        Self(Instant::now() + d)
    }
}

/// Indicates whether a ping was a success or a failure
#[derive(Debug)]
pub enum PingState {
    Ok,
    Failed(Box<dyn std::error::Error + Send>),
}

impl PingState {
    pub fn failed<E: std::error::Error + Send + 'static>(err: E) -> Self {
        Self::Failed(Box::new(err))
    }

    pub fn failed_msg<T: Into<String>>(msg: T) -> Self {
        #[derive(Debug)]
        struct PingError(String);

        impl std::fmt::Display for PingError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl std::error::Error for PingError {
            fn description(&self) -> &str {
                "ping failed"
            }

            fn cause(&self) -> Option<&dyn std::error::Error> {
                None
            }
        }

        Self::failed(PingError(msg.into()))
    }
}

/// The result of a ping. Can either be a success or a failure.
#[derive(Debug)]
pub struct Ping {
    /// Time to establish a fresh connection
    pub connect_time: Option<Duration>,
    /// Time to execute the ping
    pub latency: Option<Duration>,
    /// Total elapsed time
    pub total_time: Duration,
    pub uri: String,
    /// `Failed` or `Ok`
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

/// The current state of the pool
#[derive(Debug, Clone, Copy)]
pub struct PoolState {
    /// The number of in flight connections
    pub in_flight: usize,
    /// The total number of connections
    pub connections: usize,
    /// The number of reservations waiting for a connections
    pub reservations: usize,
    /// the number of idle connections ready to be checked out
    pub idle: usize,
    /// The number of sub pools
    pub pools: usize,
}

impl std::ops::Add for PoolState {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self {
            in_flight: self.in_flight + other.in_flight,
            reservations: self.reservations + other.reservations,
            connections: self.connections + other.connections,
            idle: self.idle + other.idle,
            pools: self.pools + other.pools,
        }
    }
}

impl Default for PoolState {
    fn default() -> Self {
        Self {
            in_flight: 0,
            reservations: 0,
            connections: 0,
            idle: 0,
            pools: 0,
        }
    }
}

impl ConnectionLike for RedisPool {
    fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
        async move {
            let mut conn = self.check_out_default().await?;
            conn.req_packed_command(cmd).await
        }
        .boxed()
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> RedisFuture<'a, Vec<Value>> {
        async move {
            let mut conn = self.check_out_default().await?;
            conn.req_packed_commands(cmd, offset, count).await
        }
        .boxed()
    }

    fn get_db(&self) -> i64 {
        -1
    }
}
