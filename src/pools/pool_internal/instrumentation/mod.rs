//! Pluggable instrumentation
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use crate::instrumentation::{Instrumentation, InstrumentationFlavour};

#[derive(Clone)]
pub(crate) struct PoolInstrumentation {
    pub pool_index: usize,
    pub flavour: InstrumentationFlavour,
    in_flight: Arc<AtomicUsize>,
}

impl PoolInstrumentation {
    pub fn new(flavour: InstrumentationFlavour, pool_index: usize) -> Self {
        Self {
            pool_index,
            flavour,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub fn pool_added(&self) {
        self.flavour.pool_added(self.pool_index)
    }

    pub fn pool_removed(&self) {
        self.flavour.pool_removed(self.pool_index);
    }

    pub fn checked_out_connection(&self, idle_for: Duration) {
        self.flavour
            .checked_out_connection(idle_for, self.pool_index)
    }
    pub fn checked_in_returned_connection(&self, flight_time: Duration) {
        self.flavour
            .checked_in_returned_connection(flight_time, self.pool_index)
    }
    pub fn checked_in_new_connection(&self) {
        self.flavour.checked_in_new_connection(self.pool_index)
    }

    pub fn connection_dropped(&self, flight_time: Option<Duration>, lifetime: Duration) {
        self.flavour
            .connection_dropped(flight_time, lifetime, self.pool_index)
    }

    pub fn connection_created(&self, connected_after: Duration, total_time: Duration) {
        self.flavour
            .connection_created(connected_after, total_time, self.pool_index)
    }
    pub fn idle_inc(&self) {
        self.flavour.idle_inc(self.pool_index)
    }
    pub fn idle_dec(&self) {
        self.flavour.idle_dec(self.pool_index)
    }

    pub fn in_flight_inc(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.flavour.in_flight_inc(self.pool_index)
    }
    pub fn in_flight_dec(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.flavour.in_flight_dec(self.pool_index)
    }

    pub fn reservation_added(&self) {
        self.flavour.reservation_added(self.pool_index)
    }
    pub fn reservation_fulfilled(&self, after: Duration) {
        self.flavour.reservation_fulfilled(after, self.pool_index)
    }
    pub fn reservation_not_fulfilled(&self, after: Duration) {
        self.flavour
            .reservation_not_fulfilled(after, self.pool_index)
    }
    pub fn reservation_limit_reached(&self) {
        self.flavour.reservation_limit_reached(self.pool_index)
    }
    pub fn connection_factory_failed(&self) {
        self.flavour.connection_factory_failed(self.pool_index)
    }
    pub fn reached_lock(&self) {
        self.flavour.reached_lock(self.pool_index)
    }
    pub fn passed_lock(&self, wait_time: Duration) {
        self.flavour.passed_lock(wait_time, self.pool_index)
    }
    pub fn lock_released(&self, exclusive_lock_time: Duration) {
        self.flavour
            .lock_released(exclusive_lock_time, self.pool_index)
    }
}
