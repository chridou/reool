//! Pluggable instrumentation
use std::sync::Arc;
use std::time::{Duration, Instant};

use log::info;

#[cfg(feature = "metrix")]
pub use self::metrix::{MetrixConfig, MetrixInstrumentation};

#[cfg(feature = "metrix")]
mod metrix;

#[derive(Clone)]
pub(crate) enum InstrumentationFlavour {
    NoInstrumentation,
    Custom(Arc<dyn Instrumentation + Sync + Send + 'static>),
    #[cfg(feature = "metrix")]
    Metrix(MetrixInstrumentation),
}

impl Instrumentation for InstrumentationFlavour {
    fn pool_added(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }

    fn pool_removed(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn checked_out_connection(&self, idle_for: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn checked_in_returned_connection(&self, flight_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn checked_in_new_connection(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn connection_dropped(&self, flight_time: Duration, _lifetime: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
        pool_index: usize,
    ) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn connection_killed(&self, _lifetime: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn idle_inc(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn idle_dec(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn reservation_added(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn reservation_fulfilled(&self, after: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn reservation_not_fulfilled(&self, after: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn reservation_limit_reached(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn connection_factory_failed(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn reached_lock(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn passed_lock(&self, wait_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
    fn lock_released(&self, exclusive_lock_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {}
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {}
        }
    }
}

/// A trait with methods that get called by the pool on certain events.
///
pub trait Instrumentation {
    fn pool_added(&self, pool_index: usize);

    fn pool_removed(&self, pool_index: usize);

    /// A connection was checked out
    fn checked_out_connection(&self, idle_for: Duration, pool_index: usize);

    /// A connection that was previously checked out was checked in again
    fn checked_in_returned_connection(&self, flight_time: Duration, pool_index: usize);

    /// A newly created connection was checked in
    fn checked_in_new_connection(&self, pool_index: usize);

    /// A connection was dropped because it was marked as defect
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration, pool_index: usize);

    /// A new connection was created
    fn connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
        pool_index: usize,
    );

    /// A connection was intentionally killed. Happens when connections are removed.
    fn connection_killed(&self, lifetime: Duration, pool_index: usize);

    /// The number of idle connections increased by 1
    fn idle_inc(&self, pool_index: usize);

    /// The number of idle connections decreased by 1
    fn idle_dec(&self, pool_index: usize);

    /// A reservation has been enqueued
    fn reservation_added(&self, pool_index: usize);

    /// A reservation was fulfilled. A connection was available in time.
    fn reservation_fulfilled(&self, after: Duration, pool_index: usize);

    /// A reservation was not fulfilled. A connection was mostly not available in time.
    fn reservation_not_fulfilled(&self, after: Duration, pool_index: usize);

    /// The reservation queue has a limit and that limit was just reached.
    /// This means a checkout has instantaneously failed.
    fn reservation_limit_reached(&self, pool_index: usize);

    /// The connection factory was asked to create a new connection but it failed to do so.
    fn connection_factory_failed(&self, pool_index: usize);

    /// A task is right about to aquire the pool
    fn reached_lock(&self, pool_index: usize);

    /// A task is right about to aquire the pool
    fn passed_lock(&self, wait_time: Duration, pool_index: usize);

    /// The time in which the lock was exclusively accessed
    fn lock_released(&self, exclusive_lock_time: Duration, pool_index: usize);
}
