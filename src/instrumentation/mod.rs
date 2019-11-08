//! Pluggable instrumentation
use std::sync::Arc;
use std::time::Duration;

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
            InstrumentationFlavour::Custom(i) => i.pool_added(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.pool_added(pool_index),
        }
    }

    fn pool_removed(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.pool_removed(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.pool_removed(pool_index),
        }
    }

    fn checked_out_connection(&self, idle_for: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.checked_out_connection(idle_for, pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.checked_out_connection(idle_for, pool_index),
        }
    }
    fn checked_in_returned_connection(&self, flight_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.checked_in_returned_connection(flight_time, pool_index)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.checked_in_returned_connection(flight_time, pool_index)
            }
        }
    }
    fn checked_in_new_connection(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.checked_in_new_connection(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.checked_in_new_connection(pool_index),
        }
    }
    fn connection_dropped(
        &self,
        flight_time: Option<Duration>,
        lifetime: Duration,
        pool_index: usize,
    ) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => {
                i.connection_dropped(flight_time, lifetime, pool_index)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.connection_dropped(flight_time, lifetime, pool_index)
            }
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
            InstrumentationFlavour::Custom(i) => {
                i.connection_created(connected_after, total_time, pool_index)
            }
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => {
                i.connection_created(connected_after, total_time, pool_index)
            }
        }
    }
    fn idle_inc(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.idle_inc(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.idle_inc(pool_index),
        }
    }
    fn idle_dec(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.idle_dec(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.idle_dec(pool_index),
        }
    }
    fn in_flight_inc(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.in_flight_inc(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.in_flight_inc(pool_index),
        }
    }
    fn in_flight_dec(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.in_flight_dec(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.in_flight_dec(pool_index),
        }
    }
    fn reservation_added(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_added(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_added(pool_index),
        }
    }
    fn reservation_fulfilled(&self, after: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_fulfilled(after, pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_fulfilled(after, pool_index),
        }
    }
    fn reservation_not_fulfilled(&self, after: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_not_fulfilled(after, pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_not_fulfilled(after, pool_index),
        }
    }
    fn reservation_limit_reached(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reservation_limit_reached(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reservation_limit_reached(pool_index),
        }
    }
    fn connection_factory_failed(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.connection_factory_failed(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.connection_factory_failed(pool_index),
        }
    }
    fn reached_lock(&self, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.reached_lock(pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.reached_lock(pool_index),
        }
    }
    fn passed_lock(&self, wait_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.passed_lock(wait_time, pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.passed_lock(wait_time, pool_index),
        }
    }
    fn lock_released(&self, exclusive_lock_time: Duration, pool_index: usize) {
        match self {
            InstrumentationFlavour::NoInstrumentation => {}
            InstrumentationFlavour::Custom(i) => i.lock_released(exclusive_lock_time, pool_index),
            #[cfg(feature = "metrix")]
            InstrumentationFlavour::Metrix(i) => i.lock_released(exclusive_lock_time, pool_index),
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
    fn connection_dropped(
        &self,
        flight_time: Option<Duration>,
        lifetime: Duration,
        pool_index: usize,
    );

    /// A new connection was created
    fn connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
        pool_index: usize,
    );

    /// The number of idle connections increased by 1
    fn idle_inc(&self, pool_index: usize);

    /// The number of idle connections decreased by 1
    fn idle_dec(&self, pool_index: usize);

    /// The number of in flight connections increased by 1
    fn in_flight_inc(&self, pool_index: usize);

    /// The number of in flight connections decreased by 1
    fn in_flight_dec(&self, pool_index: usize);

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
