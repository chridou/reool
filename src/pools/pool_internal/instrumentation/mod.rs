//! Pluggable instrumentation
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use crate::instrumentation::{Instrumentation, InstrumentationFlavour, PoolId};
use crate::PoolState;

/// Instrumentation for a single pool
///
/// Since this instrumentation is bound to a single pool
/// it has an identifier so that the metrics can be
/// aggregated with metrics from other pools.
#[derive(Clone)]
pub(crate) struct PoolInstrumentation {
    pub id: PoolId,
    pub flavour: InstrumentationFlavour,
    in_flight: Arc<AtomicUsize>,
    reservations: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    idle: Arc<AtomicUsize>,
    pools: Arc<AtomicUsize>,
}

impl PoolInstrumentation {
    pub fn new(flavour: InstrumentationFlavour, id: PoolId) -> Self {
        Self {
            id,
            flavour,
            in_flight: Arc::new(AtomicUsize::new(0)),
            pools: Arc::new(AtomicUsize::new(0)),
            reservations: Arc::new(AtomicUsize::new(0)),
            connections: Arc::new(AtomicUsize::new(0)),
            idle: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn state(&self) -> PoolState {
        PoolState {
            in_flight: self.in_flight.load(Ordering::SeqCst),
            reservations: self.reservations.load(Ordering::SeqCst),
            connections: self.connections.load(Ordering::SeqCst),
            idle: self.idle.load(Ordering::SeqCst),
            pools: self.pools.load(Ordering::SeqCst),
        }
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    pub fn pool_added(&self) {
        self.pools.fetch_add(1, Ordering::SeqCst);
        self.flavour.pool_added(self.id)
    }

    pub fn pool_removed(&self) {
        self.pools.fetch_sub(1, Ordering::SeqCst);
        self.flavour.pool_removed(self.id);
    }

    /// A connection was checked out.
    ///
    /// `idle_for`: How long has the connection been idle?
    /// `time_since_checkout_request`: How long did it take from the request to checkout
    /// until there was actually a connection checked out?
    pub fn checked_out_connection(
        &self,
        idle_for: Duration,
        time_since_checkout_request: Duration,
    ) {
        self.flavour
            .checked_out_connection(idle_for, time_since_checkout_request, self.id)
    }
    pub fn checked_in_returned_connection(&self, flight_time: Duration) {
        self.flavour
            .checked_in_returned_connection(flight_time, self.id)
    }
    pub fn checked_in_new_connection(&self) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        self.flavour.checked_in_new_connection(self.id)
    }

    pub fn connection_dropped(&self, flight_time: Option<Duration>, lifetime: Duration) {
        self.connections.fetch_sub(1, Ordering::SeqCst);
        self.flavour
            .connection_dropped(flight_time, lifetime, self.id)
    }

    pub fn connection_created(&self, connected_after: Duration, total_time: Duration) {
        self.flavour
            .connection_created(connected_after, total_time, self.id)
    }
    pub fn idle_inc(&self) {
        self.idle.fetch_add(1, Ordering::SeqCst);
        self.flavour.idle_inc(self.id)
    }

    pub fn idle_dec(&self) {
        self.idle.fetch_sub(1, Ordering::SeqCst);
        self.flavour.idle_dec(self.id)
    }

    pub fn in_flight_inc(&self) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        self.flavour.in_flight_inc(self.id)
    }
    pub fn in_flight_dec(&self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.flavour.in_flight_dec(self.id)
    }

    pub fn reservation_added(&self) {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        self.flavour.reservation_added(self.id)
    }
    pub fn reservation_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
    ) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        self.flavour
            .reservation_fulfilled(reservation_time, checkout_request_time, self.id)
    }

    pub fn reservation_not_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
    ) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        self.flavour
            .reservation_not_fulfilled(reservation_time, checkout_request_time, self.id)
    }

    pub fn reservation_limit_reached(&self) {
        self.flavour.reservation_limit_reached(self.id)
    }
    pub fn connection_factory_failed(&self) {
        self.flavour.connection_factory_failed(self.id)
    }

    pub fn internal_message_received(&self, latency: Duration) {
        self.flavour.internal_message_received(latency, self.id)
    }

    pub fn checkout_message_received(&self, latency: Duration) {
        self.flavour.checkout_message_received(latency, self.id)
    }

    pub fn relevant_message_processed(&self, processing_time: Duration) {
        self.flavour
            .relevant_message_processed(processing_time, self.id)
    }
}
