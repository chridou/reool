//! A connection pool for connecting to the nodes of a replica set
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};
use parking_lot::Mutex;
use redis::{r#async::Connection, Client, IntoConnectionInfo};

use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::{Instrumentation, NoInstrumentation};
use crate::pool::{Config as PoolConfig, MinMax, Pool};
use crate::{Checkout, RedisPool};

pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::pool::PoolStats;

/// A configuration for creating a `MultiNodePool`.
///
/// You should prefer using the `MultiNodePool::builder()` function.
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
    /// The minimum required nodes to start
    pub min_required_nodes: usize,
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

    /// Sets the maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn min_required_nodes(mut self, v: usize) -> Self {
        self.min_required_nodes = v;
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
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
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

        helpers::set_min_required_nodes(prefix, |v| {
            self.min_required_nodes = v;
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
            .min_required_nodes(self.min_required_nodes)
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
            min_required_nodes: 1,
        }
    }
}

/// A builder for a `MultiNodePool`
pub struct Builder<T, I> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    connect_to: Vec<T>,
    instrumentation: Option<I>,
}

impl Default for Builder<(), NoInstrumentation> {
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

    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub fn stats_interval(mut self, v: Duration) -> Self {
        self.config.stats_interval = v;
        self
    }

    /// The minimum required nodes to start
    pub fn min_required_nodes(mut self, v: usize) -> Self {
        self.config.min_required_nodes = v;
        self
    }

    /// The Redis nodes to connect to
    pub fn connect_to<C: IntoConnectionInfo>(self, connect_to: Vec<C>) -> Builder<C, I> {
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

    /// Updates this builder's config(not `connect_to`) from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
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
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Seperated by `;`. MANDATORY
    pub fn update_from_environment(
        self,
        prefix: Option<&str>,
    ) -> InitializationResult<Builder<String, I>> {
        let config = self.config.update_from_environment(prefix)?;

        if let Some(connect_to) = helpers::get_connect_to(prefix)? {
            Ok(Builder {
                config,
                executor_flavour: self.executor_flavour,
                connect_to,
                instrumentation: self.instrumentation,
            })
        } else {
            Err(InitializationError::message_only("'CONNECT_TO' was empty"))
        }
    }
}

impl<T, I> Builder<T, I>
where
    T: IntoConnectionInfo,
    I: Instrumentation + Send + Sync + 'static,
{
    /// Build a new `MultiNodePool`
    pub fn finish(self) -> InitializationResult<MultiNodePool> {
        MultiNodePool::create(
            self.config,
            self.connect_to,
            self.executor_flavour,
            self.instrumentation,
        )
    }
}

impl<I> Builder<String, I>
where
    I: Instrumentation + Send + Sync + 'static,
{
    /// Build a new `MultiNodePool`
    ///
    /// This is a due to a limitation that
    /// `IntoConnectionInfo` is not implemented for `String`
    pub fn finish2(self) -> InitializationResult<MultiNodePool> {
        MultiNodePool::create(
            self.config,
            self.connect_to.iter().map(|s| &**s).collect(),
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
pub struct MultiNodePool {
    inner: Arc<Inner>,
    checkout_timeout: Option<Duration>,
}

impl MultiNodePool {
    /// Creates a builder for a `MultiNodePool`
    pub fn builder() -> Builder<(), ()> {
        Builder::default()
    }

    /// Creates a new instance of a `MultiNodePool`.
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

        let instrumentation_aggregator = instrumentation
            .map(InstrumentationAggregator::new)
            .map(Arc::new);

        let mut pool_idx = 0;
        for connect_to in connect_to {
            let client = match Client::open(connect_to).map_err(InitializationError::cause_only) {
                Ok(client) => client,
                Err(err) => {
                    warn!("Failed to create a client: {}", err);
                    break;
                }
            };

            let pool_conf = PoolConfig {
                desired_pool_size: config.desired_pool_size,
                backoff_strategy: config.backoff_strategy,
                reservation_limit: config.reservation_limit,
                stats_interval: config.stats_interval,
            };

            let indexed_instrumentation = instrumentation_aggregator.as_ref().map(|agg| {
                let instr = IndexedInstrumentation::new(agg.clone(), pool_idx);
                agg.increase_pool_values();
                instr
            });

            let pool = Pool::new(
                pool_conf,
                client,
                executor_flavour.clone(),
                indexed_instrumentation,
            );

            pools.push(pool);

            pool_idx += 1;
        }

        if pools.len() < config.min_required_nodes {
            return Err(InitializationError::message_only(format!(
                "The minimum required nodes is {} but there are only {}",
                config.min_required_nodes,
                pools.len()
            )));
        }

        debug!("replica set has {} nodes", pools.len());

        Ok(Self {
            inner: Arc::new(Inner {
                count: AtomicUsize::new(0),
                pools,
            }),
            checkout_timeout: config.checkout_timeout,
        })
    }

    /// Get some statistics from each the pools.
    ///
    /// This locks the underlying pool.
    pub fn stats(&self) -> Vec<PoolStats> {
        self.inner.pools.iter().map(Pool::stats).collect()
    }
}

impl RedisPool for MultiNodePool {
    fn check_out(&self) -> Checkout {
        self.inner.check_out(self.checkout_timeout)
    }

    fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        self.inner.check_out(timeout)
    }
}

struct Inner {
    count: AtomicUsize,
    pools: Vec<Pool<Connection>>,
}

impl Inner {
    fn check_out(&self, checkout_timeout: Option<Duration>) -> Checkout {
        let count = self.count.fetch_add(1, Ordering::SeqCst);

        let idx = count % self.pools.len();

        Checkout(self.pools[idx].check_out(checkout_timeout))
    }
}

struct InstrumentationAggregator<I> {
    outbound: I,
    tracking: Mutex<Tracking>,
}

struct Tracking {
    pool_size: ValueTracker,
    idle: ValueTracker,
    in_flight: ValueTracker,
    reservations: ValueTracker,
}

impl<I> InstrumentationAggregator<I>
where
    I: Instrumentation + Send + Sync,
{
    pub fn new(instrumentation: I) -> Self {
        InstrumentationAggregator {
            outbound: instrumentation,
            tracking: Mutex::new(Tracking {
                pool_size: ValueTracker::default(),
                idle: ValueTracker::default(),
                in_flight: ValueTracker::default(),
                reservations: ValueTracker::default(),
            }),
        }
    }

    pub fn increase_pool_values(&self) {
        let mut tracking = self.tracking.lock();
        tracking.pool_size.add_pool();
        tracking.idle.add_pool();
        tracking.in_flight.add_pool();
        tracking.reservations.add_pool();
    }
}

impl<I> InstrumentationAggregator<I>
where
    I: Instrumentation,
{
    fn checked_out_connection(&self, _pool_idx: usize) {
        self.outbound.checked_out_connection()
    }
    fn checked_in_returned_connection(&self, flight_time: Duration, _pool_idx: usize) {
        self.outbound.checked_in_returned_connection(flight_time)
    }
    fn checked_in_new_connection(&self, _pool_idx: usize) {
        self.outbound.checked_in_new_connection()
    }
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration, _pool_idx: usize) {
        self.outbound.connection_dropped(flight_time, lifetime)
    }
    fn connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
        _pool_idx: usize,
    ) {
        self.outbound
            .connection_created(connected_after, total_time)
    }
    fn connection_killed(&self, lifetime: Duration, _pool_idx: usize) {
        self.outbound.connection_killed(lifetime)
    }
    fn reservation_added(&self, _pool_idx: usize) {
        self.outbound.reservation_added()
    }
    fn reservation_fulfilled(&self, after: Duration, _pool_idx: usize) {
        self.outbound.reservation_fulfilled(after)
    }
    fn reservation_not_fulfilled(&self, after: Duration, _pool_idx: usize) {
        self.outbound.reservation_not_fulfilled(after)
    }
    fn reservation_limit_reached(&self, _pool_idx: usize) {
        self.outbound.reservation_limit_reached()
    }
    fn connection_factory_failed(&self, _pool_idx: usize) {
        self.outbound.connection_factory_failed()
    }

    fn stats(&self, stats: PoolStats, pool_idx: usize) {
        let mut tracking = self.tracking.lock();
        let pool_size = tracking.pool_size.update(pool_idx, stats.pool_size);
        let idle = tracking.idle.update(pool_idx, stats.idle);
        let reservations = tracking.idle.update(pool_idx, stats.reservations);
        let in_flight = tracking.idle.update(pool_idx, stats.in_flight);

        let stats = PoolStats {
            pool_size,
            reservations,
            idle,
            in_flight,
            node_count: tracking.pool_size.node_count(),
        };

        self.outbound.stats(stats)
    }
}

struct ValueTracker {
    pool_values: Vec<MinMax>,
}

impl ValueTracker {
    pub fn add_pool(&mut self) {
        self.pool_values.push(MinMax::default())
    }

    pub fn update(&mut self, idx: usize, v: MinMax) -> MinMax {
        self.pool_values[idx] = v;
        let curr_min = self.pool_values.iter().map(|v| v.min()).min().unwrap_or(0);
        let curr_max = self.pool_values.iter().map(|v| v.max()).max().unwrap_or(0);
        MinMax(curr_min, curr_max)
    }

    pub fn node_count(&self) -> usize {
        self.pool_values.len()
    }
}

impl Default for ValueTracker {
    fn default() -> Self {
        Self {
            pool_values: Vec::new(),
        }
    }
}

struct IndexedInstrumentation<I> {
    index: usize,
    aggregator: Arc<InstrumentationAggregator<I>>,
}

impl<I> IndexedInstrumentation<I>
where
    I: Instrumentation,
{
    pub fn new(aggregator: Arc<InstrumentationAggregator<I>>, index: usize) -> Self {
        Self { index, aggregator }
    }
}

impl<I> Instrumentation for IndexedInstrumentation<I>
where
    I: Instrumentation,
{
    fn checked_out_connection(&self) {
        self.aggregator.checked_out_connection(self.index)
    }
    fn checked_in_returned_connection(&self, flight_time: Duration) {
        self.aggregator
            .checked_in_returned_connection(flight_time, self.index)
    }
    fn checked_in_new_connection(&self) {
        self.aggregator.checked_in_new_connection(self.index)
    }
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration) {
        self.aggregator
            .connection_dropped(flight_time, lifetime, self.index)
    }
    fn connection_created(&self, connected_after: Duration, total_time: Duration) {
        self.aggregator
            .connection_created(connected_after, total_time, self.index)
    }
    fn connection_killed(&self, lifetime: Duration) {
        self.aggregator.connection_killed(lifetime, self.index)
    }
    fn reservation_added(&self) {
        self.aggregator.reservation_added(self.index)
    }
    fn reservation_fulfilled(&self, after: Duration) {
        self.aggregator.reservation_fulfilled(after, self.index)
    }
    fn reservation_not_fulfilled(&self, after: Duration) {
        self.aggregator.reservation_not_fulfilled(after, self.index)
    }
    fn reservation_limit_reached(&self) {
        self.aggregator.reservation_limit_reached(self.index)
    }
    fn connection_factory_failed(&self) {
        self.aggregator.connection_factory_failed(self.index)
    }
    fn stats(&self, stats: PoolStats) {
        self.aggregator.stats(stats, self.index)
    }
}
