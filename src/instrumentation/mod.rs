//! Pluggable instrumentation
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "metrix")]
pub use self::metrix::{MetrixConfig, MetrixInstrumentation};
pub use state_counters::*;

#[cfg(feature = "metrix")]
mod metrix;
mod state_counters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PoolId(usize);

impl PoolId {
    pub fn new(id: usize) -> Self {
        PoolId(id)
    }

    pub fn into_inner(self) -> usize {
        self.0
    }

    pub fn inc(&mut self) {
        self.0 += 1;
    }
}

impl From<PoolId> for usize {
    fn from(pid: PoolId) -> Self {
        pid.0
    }
}

impl fmt::Display for PoolId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "P{:04}", self.0)
    }
}

/// A trait with methods that get called by the pool on certain events.
///
pub trait Instrumentation {
    fn pool_added(&self, pool: PoolId);

    fn pool_removed(&self, pool: PoolId);

    /// A connection was checked out
    fn checked_out_connection(
        &self,
        idle_for: Duration,
        time_since_checkout_request: Duration,
        pool: PoolId,
    );

    /// A connection that was previously checked out was checked in again
    fn checked_in_returned_connection(&self, flight_time: Duration, pool: PoolId);

    /// A newly created connection was checked in
    fn checked_in_new_connection(&self, pool: PoolId);

    /// A connection was dropped because it was marked as defect
    fn connection_dropped(&self, flight_time: Option<Duration>, lifetime: Duration, pool: PoolId);

    /// A new connection was created
    fn connection_created(&self, connected_after: Duration, total_time: Duration, pool: PoolId);

    /// The number of idle connections increased by 1
    fn idle_inc(&self, pool: PoolId);

    /// The number of idle connections decreased by 1
    fn idle_dec(&self, pool: PoolId);

    /// The number of in flight connections increased by 1
    fn in_flight_inc(&self, pool: PoolId);

    /// The number of in flight connections decreased by 1
    fn in_flight_dec(&self, pool: PoolId);

    /// A reservation has been enqueued
    fn reservation_added(&self, pool: PoolId);

    /// A reservation was fulfilled. A connection was available in time.
    fn reservation_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
        pool: PoolId,
    );

    /// A reservation was not fulfilled. A connection was mostly not available in time.
    fn reservation_not_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
        pool: PoolId,
    );

    /// The reservation queue has a limit and that limit was just reached.
    /// This means a checkout has instantaneously failed.
    fn reservation_limit_reached(&self, pool: PoolId);

    /// The connection factory was asked to create a new connection but it failed to do so.
    fn connection_factory_failed(&self, pool: PoolId);

    /// A pool internal message was received
    fn internal_message_received(&self, latency: Duration, pool: PoolId);

    fn checkout_message_received(&self, latency: Duration, pool: PoolId);

    fn relevant_message_processed(&self, processing_time: Duration, pool: PoolId);
}

#[derive(Clone)]
pub(crate) enum InstrumentationFlavour {
    NoInstrumentation,
    Custom(Arc<dyn Instrumentation + Sync + Send + 'static>),
    #[cfg(feature = "metrix")]
    Metrix(MetrixInstrumentation),
}

impl Instrumentation for InstrumentationFlavour {
    fn pool_added(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.pool_added(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.pool_added(pool),
        }
    }

    fn pool_removed(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.pool_removed(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.pool_removed(pool),
        }
    }

    fn checked_out_connection(
        &self,
        idle_for: Duration,
        time_since_checkout_request: Duration,
        pool: PoolId,
    ) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.checked_out_connection(idle_for, time_since_checkout_request, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.checked_out_connection(idle_for, time_since_checkout_request, pool)
            }
        }
    }
    fn checked_in_returned_connection(&self, flight_time: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.checked_in_returned_connection(flight_time, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.checked_in_returned_connection(flight_time, pool)
            }
        }
    }
    fn checked_in_new_connection(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.checked_in_new_connection(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.checked_in_new_connection(pool),
        }
    }
    fn connection_dropped(&self, flight_time: Option<Duration>, lifetime: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.connection_dropped(flight_time, lifetime, pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.connection_dropped(flight_time, lifetime, pool),
        }
    }
    fn connection_created(&self, connected_after: Duration, total_time: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.connection_created(connected_after, total_time, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.connection_created(connected_after, total_time, pool)
            }
        }
    }
    fn idle_inc(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.idle_inc(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.idle_inc(pool),
        }
    }
    fn idle_dec(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.idle_dec(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.idle_dec(pool),
        }
    }
    fn in_flight_inc(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.in_flight_inc(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.in_flight_inc(pool),
        }
    }
    fn in_flight_dec(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.in_flight_dec(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.in_flight_dec(pool),
        }
    }
    fn reservation_added(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_added(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_added(pool),
        }
    }
    fn reservation_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
        pool: PoolId,
    ) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.reservation_fulfilled(reservation_time, checkout_request_time, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.reservation_fulfilled(reservation_time, checkout_request_time, pool)
            }
        }
    }
    fn reservation_not_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
        pool: PoolId,
    ) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.reservation_not_fulfilled(reservation_time, checkout_request_time, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.reservation_not_fulfilled(reservation_time, checkout_request_time, pool)
            }
        }
    }
    fn reservation_limit_reached(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_limit_reached(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_limit_reached(pool),
        }
    }
    fn connection_factory_failed(&self, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.connection_factory_failed(pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.connection_factory_failed(pool),
        }
    }

    fn internal_message_received(&self, latency: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.internal_message_received(latency, pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.internal_message_received(latency, pool),
        }
    }

    fn checkout_message_received(&self, latency: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.checkout_message_received(latency, pool),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.checkout_message_received(latency, pool),
        }
    }

    fn relevant_message_processed(&self, processing_time: Duration, pool: PoolId) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.relevant_message_processed(processing_time, pool)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.relevant_message_processed(processing_time, pool)
            }
        }
    }
}
