use std::sync::Arc;
use std::time::Duration;

use crate::stats::{MinMax, PoolStats};
use parking_lot::Mutex;

use crate::instrumentation::Instrumentation;

pub struct InstrumentationAggregator<I> {
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

pub struct IndexedInstrumentation<I> {
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
