//! A connection pool for connecting to the nodes of a replica set
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{self, Future};
use log::{debug, warn};
use parking_lot::Mutex;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationError;
use crate::error::InitializationResult;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pool_internal::{CheckoutManaged, Config as PoolConfig, PoolInternal};
use crate::pooled_connection::ConnectionFlavour;
use crate::stats::{MinMax, PoolStats};
use crate::{Checkout, Ping};

/// A connection pool that maintains multiple connections
/// to a multiple Redis instances containing the same data(replicas).
///
/// All the instances should be part of the same replica set.
/// You should only perform read operations on the
/// connections received from this kind of pool.
///
/// The replicas are selected in a round robin fashion.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub(crate) struct MultiNodePool {
    inner: Arc<Inner>,
    checkout_timeout: Option<Duration>,
}

impl MultiNodePool {
    pub(crate) fn create<I, F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<MultiNodePool>
    where
        I: Instrumentation + Send + Sync + 'static,
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(String) -> InitializationResult<CF>,
    {
        let mut pools = Vec::new();

        let instrumentation_aggregator = instrumentation
            .map(InstrumentationAggregator::new)
            .map(Arc::new);

        let mut pool_idx = 0;
        for connect_to in config.connect_to_nodes {
            let connection_factory = create_connection_factory(connect_to)?;
            let pool_conf = PoolConfig {
                desired_pool_size: config.desired_pool_size,
                backoff_strategy: config.backoff_strategy,
                reservation_limit: config.reservation_limit,
                stats_interval: config.stats_interval,
                activation_order: config.activation_order,
            };

            let indexed_instrumentation = instrumentation_aggregator.as_ref().map(|agg| {
                let instr = IndexedInstrumentation::new(agg.clone(), pool_idx);
                agg.add_new_pool();
                instr
            });

            let pool = PoolInternal::new(
                pool_conf,
                connection_factory,
                executor_flavour.clone(),
                indexed_instrumentation,
            );

            pools.push(pool);

            pool_idx += 1;
        }

        if pools.len() < config.min_required_nodes {
            return Err(InitializationError::message_only(format!(
                "the minimum required nodes is {} but there are only {}",
                config.min_required_nodes,
                pools.len()
            )));
        } else if pools.is_empty() {
            warn!("no nodes is allowed and there are no nodes.");
        }

        debug!("replica set has {} nodes", pools.len());

        let inner = Inner {
            count: AtomicUsize::new(0),
            pools,
        };

        Ok(MultiNodePool {
            inner: Arc::new(inner),
            checkout_timeout: config.checkout_timeout,
        })
    }

    pub fn check_out(&self) -> Checkout {
        self.inner.check_out(self.checkout_timeout)
    }

    pub fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        self.inner.check_out(timeout)
    }

    /// Get some statistics from each of the pools.
    ///
    /// This locks the underlying pool.
    pub fn stats(&self) -> Vec<PoolStats> {
        self.inner.pools.iter().map(PoolInternal::stats).collect()
    }

    /// Triggers the pool to emit statistics if `stats_interval` has elapsed.
    ///
    /// This locks the underlying pool.
    pub fn trigger_stats(&self) {
        self.inner
            .pools
            .iter()
            .for_each(PoolInternal::trigger_stats)
    }

    pub fn ping(
        &self,
        timeout: Option<Duration>,
    ) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        self.inner.ping(timeout)
    }
}

impl Clone for MultiNodePool {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            checkout_timeout: self.checkout_timeout,
        }
    }
}

struct Inner {
    count: AtomicUsize,
    pools: Vec<PoolInternal<ConnectionFlavour>>,
}

impl Inner {
    fn check_out(&self, checkout_timeout: Option<Duration>) -> Checkout {
        if self.pools.is_empty() {
            Checkout(CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::NoConnection,
            ))))
        } else {
            let count = self.count.fetch_add(1, Ordering::SeqCst);

            let idx = count % self.pools.len();

            Checkout(self.pools[idx].check_out(checkout_timeout))
        }
    }

    pub fn ping(
        &self,
        timeout: Option<Duration>,
    ) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        self.pool.ping(timeout)
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

    pub fn add_new_pool(&self) {
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
    fn checked_out_connection(&self, idle_for: Duration, _pool_idx: usize) {
        self.outbound.checked_out_connection(idle_for)
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
        let reservations = tracking.reservations.update(pool_idx, stats.reservations);
        let in_flight = tracking.in_flight.update(pool_idx, stats.in_flight);
        let node_count = tracking.pool_size.node_count();
        drop(tracking);

        let stats = PoolStats {
            pool_size,
            reservations,
            idle,
            in_flight,
            node_count,
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
        let curr_min = self.pool_values.iter().map(MinMax::min).min().unwrap_or(0);
        let curr_max = self.pool_values.iter().map(MinMax::max).max().unwrap_or(0);
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
    fn checked_out_connection(&self, idle_for: Duration) {
        self.aggregator.checked_out_connection(idle_for, self.index)
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
