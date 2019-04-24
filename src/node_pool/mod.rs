//! A connection pool for conencting to a single node
use std::time::Duration;

use redis::{r#async::Connection, Client, IntoConnectionInfo};

use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::Instrumentation;
use crate::pool::{Config as PoolConfig, Pool, PoolStats};
use crate::{Checkout, RedisPool};

pub use crate::backoff_strategy::BackoffStrategy;

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

    /// Updates this configuration from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    pub fn update_from_environment<T: Into<String>>(
        mut self,
        prefix: Option<T>,
    ) -> InitializationResult<Self> {
        let prefix = prefix.map(Into::into);

        helpers::set_desired_pool_size(prefix.clone(), |v| {
            self.desired_pool_size = v;
        })?;

        helpers::set_checkout_timeout(prefix.clone(), |v| {
            self.checkout_timeout = v;
        })?;

        helpers::set_reservation_limit(prefix, |v| {
            self.reservation_limit = v;
        })?;

        Ok(self)
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder<(), ()> {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .checkout_timeout(self.checkout_timeout)
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            checkout_timeout: Some(Duration::from_millis(20)),
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: Some(100),
        }
    }
}

/// A builder for a `SingleNodePool`
pub struct Builder<T, I> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    connect_to: T,
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

    /// The Redis node to connect to
    pub fn connect_to<C: IntoConnectionInfo>(self, connect_to: C) -> Builder<C, I> {
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to,
            instrumentation: self.instrumentation,
        }
    }

    /// The exucutor to use for spawning tasks. If not set it is assumed
    /// that the poolis created on the default runtime.
    pub fn task_executor(mut self, executor: ::tokio::runtime::TaskExecutor) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioTaskExecutor(executor);
        self
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
    ) -> Builder<T, crate::instrumentation::metrix::MetrixInstrumentation> {
        let instrumentation = crate::instrumentation::metrix::create(aggregates_processors);
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to: self.connect_to,
            instrumentation: Some(instrumentation),
        }
    }
}

impl<T, I> Builder<T, I>
where
    T: IntoConnectionInfo,
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
    pub fn new<T>(config: Config, connect_to: T) -> InitializationResult<Self>
    where
        T: IntoConnectionInfo,
    {
        Self::create::<T, ()>(config, connect_to, ExecutorFlavour::Runtime, None)
    }

    pub(crate) fn create<T, I>(
        config: Config,
        connect_to: T,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<Self>
    where
        T: IntoConnectionInfo,
        I: Instrumentation + Send + Sync + 'static,
    {
        if config.desired_pool_size == 0 {
            return Err(InitializationError::message_only(
                "'desired_pool_size' must be at least 1",
            ));
        }

        let client =
            Client::open(connect_to).map_err(|err| InitializationError::cause_only(err))?;

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            reservation_limit: config.reservation_limit,
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
            let _ = self.pool.add_new_connection();
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
    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
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
