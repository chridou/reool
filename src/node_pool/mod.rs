use std::sync::Arc;
use std::time::Duration;

use redis::r#async::Connection;

use crate::backoff_strategy::BackoffStrategy;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pool::{Config as PoolConfig, Pool, PoolStats};
use crate::*;

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
            connect_to: connect_to,
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
    pub fn instrumented<II>(mut self, instrumentation: II) -> Builder<T, II>
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

/// A connection pool that maintains multiple connections
/// to a single Redis instance.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
#[derive(Clone)]
pub struct SingleNodePool {
    pool: Arc<Pool<Connection>>,
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
            pool: Arc::new(pool),
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
    /// Do not call this function when there are no more connections
    /// managed by the pool.
    ///
    /// This migt not happen immediately.
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
