//! A connection pool for conencting to a single node
use std::time::Duration;

use redis::{r#async::Connection, Client};

use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::{Instrumentation, NoInstrumentation};
use crate::pool::{Config as PoolConfig, Pool};
use crate::{Checkout, RedisPool};

pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::pool::PoolStats;

/// A configuration for creating a `SingleNodePool`.
///
/// You should prefer using the `SingleNodePool::builder()` function.
pub struct Config {
    /// The number of connections the pool should initially have
    /// and try to maintain
    pub desired_pool_size: usize,
    /// The timeout for a checkout if no specific tinmeout is given
    /// with a checkout.
    pub checkout_timeout: Option<Duration>,
    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub backoff_strategy: BackoffStrategy,
    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub reservation_limit: Option<usize>,
    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub stats_interval: Duration,
}

impl Config {
    /// Sets the number of connections the pool should initially have
    /// and try to maintain
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.desired_pool_size = v;
        self
    }

    /// Sets the timeout for a checkout if no specific tinmeout is given
    /// with a checkout.
    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.checkout_timeout = v;
        self
    }

    /// Sets the `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.backoff_strategy = v;
        self
    }

    /// Sets the maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn reservation_limit(mut self, v: Option<usize>) -> Self {
        self.reservation_limit = v;
        self
    }

    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub fn stats_interval(mut self, v: Duration) -> Self {
        self.stats_interval = v;
        self
    }

    /// Updates this configuration from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    pub fn update_from_environment(mut self, prefix: Option<&str>) -> InitializationResult<Self> {
        helpers::set_desired_pool_size(prefix, |v| {
            self.desired_pool_size = v;
        })?;

        helpers::set_checkout_timeout(prefix, |v| {
            self.checkout_timeout = v;
        })?;

        helpers::set_reservation_limit(prefix, |v| {
            self.reservation_limit = v;
        })?;

        helpers::set_stats_interval(prefix, |v| {
            self.stats_interval = v;
        })?;

        Ok(self)
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder<(), NoInstrumentation> {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .checkout_timeout(self.checkout_timeout)
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
            .stats_interval(self.stats_interval)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            checkout_timeout: Some(Duration::from_millis(20)),
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: Some(100),
            stats_interval: Duration::from_millis(100),
        }
    }
}

/// A builder for a `SingleNodePool`
pub struct Builder<C, I> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    connect_to: C,
    instrumentation: Option<I>,
}

impl Default for Builder<(), ()> {
    fn default() -> Self {
        Self {
            config: Config::default(),
            executor_flavour: ExecutorFlavour::Runtime,
            connect_to: (),
            instrumentation: None,
        }
    }
}

impl<T, I> Builder<T, I> {
    /// The number of connections the pool should initially have
    /// and try to maintain
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.config.desired_pool_size = v;
        self
    }

    /// The timeout for a checkout if no specific tinmeout is given
    /// with a checkout.
    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.config.checkout_timeout = v;
        self
    }

    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.config.backoff_strategy = v;
        self
    }

    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn reservation_limit(mut self, v: Option<usize>) -> Self {
        self.config.reservation_limit = v;
        self
    }

    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub fn stats_interval(mut self, v: Duration) -> Self {
        self.config.stats_interval = v;
        self
    }

    /// The Redis node to connect to
    pub fn connect_to<T: Into<String>>(self, connect_to: T) -> Builder<String, I> {
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to: connect_to.into(),
            instrumentation: self.instrumentation,
        }
    }

    /// The exucutor to use for spawning tasks. If not set it is assumed
    /// that the poolis created on the default runtime.
    pub fn task_executor(mut self, executor: ::tokio::runtime::TaskExecutor) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioTaskExecutor(executor);
        self
    }

    /// Updates this builder's config(not `connect_to`) from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    pub fn update_config_from_environment(
        self,
        prefix: Option<&str>,
    ) -> InitializationResult<Builder<T, I>> {
        let config = self.config.update_from_environment(prefix)?;

        Ok(Builder {
            config,
            executor_flavour: self.executor_flavour,
            connect_to: self.connect_to,
            instrumentation: self.instrumentation,
        })
    }

    /// Adds instrumentation to the pool
    pub fn instrumented<II>(self, instrumentation: II) -> Builder<T, II>
    where
        II: Instrumentation + Send + Sync + 'static,
    {
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to: self.connect_to,
            instrumentation: Some(instrumentation),
        }
    }

    #[cfg(feature = "metrix")]
    pub fn instrumented_with_metrix<A: metrix::processor::AggregatesProcessors>(
        self,
        aggregates_processors: &mut A,
        config: crate::instrumentation::MetrixConfig,
    ) -> Builder<T, crate::instrumentation::metrix::MetrixInstrumentation> {
        let instrumentation = crate::instrumentation::metrix::create(aggregates_processors, config);
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to: self.connect_to,
            instrumentation: Some(instrumentation),
        }
    }
}

impl<I> Builder<(), I>
where
    I: Instrumentation + Send + Sync + 'static,
{
    /// Updates this builder from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Seperated by `;`. MANDATORY. If there is a list, the first
    /// entry is chosen
    pub fn update_from_environment(
        self,
        prefix: Option<&str>,
    ) -> InitializationResult<Builder<String, I>> {
        let config = self.config.update_from_environment(prefix)?;

        if let Some(mut connect_to) = helpers::get_connect_to(prefix)? {
            if connect_to.is_empty() {
                Err(InitializationError::message_only(
                    "'CONNECT_TO' was found but empty",
                ))
            } else {
                Ok(Builder {
                    config,
                    executor_flavour: self.executor_flavour,
                    connect_to: connect_to.remove(0),
                    instrumentation: self.instrumentation,
                })
            }
        } else {
            Err(InitializationError::message_only("'CONNECT_TO' was empty"))
        }
    }
}

impl<I> Builder<String, I>
where
    I: Instrumentation + Send + Sync + 'static,
{
    /// Build a new `SingleNodePool`
    pub fn finish(self) -> InitializationResult<SingleNodePool> {
        SingleNodePool::create(
            self.config,
            self.connect_to,
            self.executor_flavour,
            self.instrumentation,
        )
    }
}

/// A connection pool that maintains multiple connections
/// to a single Redis instance.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
#[derive(Clone)]
pub struct SingleNodePool {
    pool: Pool<Connection>,
    checkout_timeout: Option<Duration>,
}

impl SingleNodePool {
    /// Creates a builder for a `SingleNodePool`
    pub fn builder() -> Builder<(), ()> {
        Builder::default()
    }

    /// Creates a new instance of a `SingleNodePool`.
    ///
    /// This function must be
    /// called on a thread of the tokio runtime.
    pub fn new<C>(config: Config, connect_to: C) -> InitializationResult<Self>
    where
        C: Into<String>,
    {
        Self::create::<NoInstrumentation>(config, connect_to.into(), ExecutorFlavour::Runtime, None)
    }

    pub(crate) fn create<I>(
        config: Config,
        connect_to: String,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<Self>
    where
        I: Instrumentation + Send + Sync + 'static,
    {
        if config.desired_pool_size == 0 {
            return Err(InitializationError::message_only(
                "'desired_pool_size' must be at least 1",
            ));
        }

        let client = Client::open(&*connect_to).map_err(InitializationError::cause_only)?;

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            reservation_limit: config.reservation_limit,
            stats_interval: config.stats_interval,
        };

        let pool = Pool::new(pool_conf, client, executor_flavour, instrumentation);

        Ok(Self {
            pool,
            checkout_timeout: config.checkout_timeout,
        })
    }

    /// Add `n` new connections to the pool.
    ///
    /// This might not happen immediately.
    pub fn add_connections(&self, n: usize) {
        (0..n).for_each(|_| {
            self.pool.add_new_connection();
        });
    }

    /// Remove a connection from the pool.
    ///
    /// This might not happen immediately.
    ///
    /// Do not call this function when there are no more connections
    /// managed by the pool. The requests to reduce the
    /// number of connections will are taken from a queue.
    pub fn remove_connection(&self) {
        self.pool.remove_connection();
    }

    /// Get some statistics from the pool.
    ///
    /// This locks the underlying pool.
    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
    }

    /// Triggers the pool to emit statistics if `stats_interval` has elapsed.
    ///
    /// This locks the underlying pool.
    pub fn trigger_stats(&self) {
        self.pool.trigger_stats()
    }
}

impl RedisPool for SingleNodePool {
    fn check_out(&self) -> Checkout {
        Checkout(self.pool.check_out(self.checkout_timeout))
    }

    fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        Checkout(self.pool.check_out(timeout))
    }
}
