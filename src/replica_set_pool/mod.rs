//! A connection pool for connecting to the nodes of a replica set

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use redis::{r#async::Connection, Client, IntoConnectionInfo};

use crate::backoff_strategy::BackoffStrategy;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pool::{Config as PoolConfig, Pool, PoolStats};
use crate::{Checkout, RedisPool};

/// A configuration for creating a `ReplicaSetPool`.
///
/// You should prefer using the `ReplicaSetPool::builder()` function.
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

/// A builder for a `ReplicaSetPool`
pub struct Builder<T, I> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    connect_to: Vec<T>,
    instrumentation: Option<I>,
}

impl Default for Builder<(), ()> {
    fn default() -> Self {
        Self {
            config: Config::default(),
            executor_flavour: ExecutorFlavour::Runtime,
            connect_to: Vec::default(),
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

    /// The Redis nodes to connect to
    pub fn connect_to<C: IntoConnectionInfo>(self, connect_to: Vec<C>) -> Builder<C, I> {
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
    /// Build a new `ReplicaSetPool`
    pub fn finish(self) -> InitializationResult<ReplicaSetPool> {
        ReplicaSetPool::create(
            self.config,
            self.connect_to,
            self.executor_flavour,
            self.instrumentation,
        )
    }
}

/// A connection pool that maintains multiple connections
/// to a multiple Redis instances. All the instances should
/// be part of the same replica set. You should only perform
/// read operations on the connections received from this kind of pool.
///
/// The replicas are selected in a round robin fashion.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
#[derive(Clone)]
pub struct ReplicaSetPool {
    inner: Arc<Inner>,
    checkout_timeout: Option<Duration>,
}

impl ReplicaSetPool {
    /// Creates a builder for a `ReplicaSetPool`
    pub fn builder() -> Builder<(), ()> {
        Builder::default()
    }

    /// Creates a new instance of a `ReplicaSetPool`.
    ///
    /// This function must be
    /// called on a thread of the tokio runtime.
    pub fn new<T>(config: Config, connect_to: Vec<T>) -> InitializationResult<Self>
    where
        T: IntoConnectionInfo,
    {
        Self::create::<T, ()>(config, connect_to, ExecutorFlavour::Runtime, None)
    }

    pub(crate) fn create<T, I>(
        config: Config,
        connect_to: Vec<T>,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<Self>
    where
        T: IntoConnectionInfo,
        I: Instrumentation + Send + Sync + 'static,
    {
        if connect_to.is_empty() {
            return Err(InitializationError::message_only(
                "There must be at least on node to connect to.",
            ));
        }

        let mut pools = Vec::new();

        let instrumentation = instrumentation.map(InstrumentationInterceptor::new);

        for connect_to in connect_to {
            let client =
                Client::open(connect_to).map_err(|err| InitializationError::cause_only(err))?;

            let pool_conf = PoolConfig {
                desired_pool_size: config.desired_pool_size,
                backoff_strategy: config.backoff_strategy,
                reservation_limit: config.reservation_limit,
            };

            let pool = Pool::new(
                pool_conf,
                client,
                executor_flavour.clone(),
                instrumentation.clone(),
            );

            pools.push(pool);
        }

        Ok(Self {
            inner: Arc::new(Inner {
                conn_count: AtomicUsize::new(0),
                pools,
            }),
            checkout_timeout: config.checkout_timeout,
        })
    }

    /// Get some statistics from each the pools.
    pub fn stats(&self) -> Vec<PoolStats> {
        self.inner.pools.iter().map(|p| p.stats()).collect()
    }
}

impl RedisPool for ReplicaSetPool {
    fn check_out(&self) -> Checkout {
        self.inner.check_out(self.checkout_timeout)
    }

    fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        self.inner.check_out(timeout)
    }
}

struct Inner {
    conn_count: AtomicUsize,
    pools: Vec<Pool<Connection>>,
}

impl Inner {
    fn check_out(&self, checkout_timeout: Option<Duration>) -> Checkout {
        let conn_count = self.conn_count.fetch_add(1, Ordering::SeqCst);

        let idx = self.pools.len() % conn_count;

        Checkout(self.pools[idx].check_out(checkout_timeout))
    }
}

struct InstrumentationInterceptor<I> {
    inner: Arc<I>,
}

impl<I> Clone for InstrumentationInterceptor<I> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<I> InstrumentationInterceptor<I>
where
    I: Instrumentation + Send + Sync,
{
    pub fn new(instrumentation: I) -> Self {
        InstrumentationInterceptor {
            inner: Arc::new(instrumentation),
        }
    }
}

impl<I> Instrumentation for InstrumentationInterceptor<I>
where
    I: Instrumentation,
{
    fn checked_out_connection(&self) {
        self.inner.checked_out_connection()
    }
    fn checked_in_returned_connection(&self, flight_time: Duration) {
        self.inner.checked_in_returned_connection(flight_time)
    }
    fn checked_in_new_connection(&self) {
        self.inner.checked_in_new_connection()
    }
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration) {
        self.inner.connection_dropped(flight_time, lifetime)
    }
    fn idle_connections_changed(&self, len: usize) {
        self.inner.idle_connections_changed(len)
    }
    fn connection_created(&self, connected_after: Duration, total_time: Duration) {
        self.inner.connection_created(connected_after, total_time)
    }
    fn killed_connection(&self, lifetime: Duration) {
        self.inner.killed_connection(lifetime)
    }
    fn reservations_changed(&self, len: usize) {
        self.inner.reservations_changed(len)
    }
    fn reservation_added(&self) {
        self.inner.reservation_added()
    }
    fn reservation_fulfilled(&self, after: Duration) {
        self.inner.reservation_fulfilled(after)
    }
    fn reservation_not_fulfilled(&self, after: Duration) {
        self.inner.reservation_not_fulfilled(after)
    }
    fn reservation_limit_reached(&self) {
        self.inner.reservation_limit_reached()
    }
    fn connection_factory_failed(&self) {
        self.inner.connection_factory_failed()
    }
}
