use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use log::info;

use crate::instrumentation::PoolId;
use crate::PoolState;

use super::Instrumentation;

/// Simply tracks the following values:
///
/// * number of existing connections
/// * number of idle connections
/// * number of in flight connections
/// * number of reservations
/// * number of tasks waiting before the lock
///
/// This is mostly useful for testing purposes only.
///
/// Call the `instrumentation` method to create an
/// `Instrumentation` that can be put into a pool.
///
/// The logged or printed output might not be accurate
/// since there are multiple race conditions possible inside
/// the `StateCounters`. This is desired behaviour to not
/// block threads. So the values you see in the output
/// might already have changed to something different when
/// logged.
#[derive(Clone, Default)]
pub struct StateCounters {
    connections: Arc<AtomicUsize>,
    idle: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    reservations: Arc<AtomicUsize>,
    pools: Arc<AtomicUsize>,
    log: bool,
    print: bool,
}

impl StateCounters {
    /// Create a new `StateCounter`.
    ///
    /// This method checks the environment:
    ///
    /// * If an env var "PRINT" is set, counter changes will be printed to stdout.
    /// * If an env var "LOG" or "RUST_LOG" is set counter changes will be logged
    /// at info level
    pub fn new() -> Self {
        let mut me = Self::default();
        if std::env::var("RUST_LOG").is_ok() {
            me.log = true;
        }

        if std::env::var("LOG").is_ok() {
            me.log = true;
        }

        if std::env::var("PRINT").is_ok() {
            me.print = true;
        }

        me
    }

    /// Log counter changes at info level
    pub fn with_logging() -> Self {
        let mut me = Self::default();
        me.log = true;
        me
    }

    /// Print counter changes to stdout
    pub fn with_printing() -> Self {
        let mut me = Self::default();
        me.print = true;
        me
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

    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
    pub fn idle(&self) -> usize {
        self.idle.load(Ordering::SeqCst)
    }
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }
    pub fn reservations(&self) -> usize {
        self.reservations.load(Ordering::SeqCst)
    }
    pub fn pools(&self) -> usize {
        self.pools.load(Ordering::SeqCst)
    }

    /// Create the `Instrumentation` to be put into the pool to instrument.
    pub fn instrumentation(&self) -> StateCountersInstrumentation {
        StateCountersInstrumentation {
            connections: Arc::clone(&self.connections),
            idle: Arc::clone(&self.idle),
            in_flight: Arc::clone(&self.in_flight),
            reservations: Arc::clone(&self.reservations),
            pools: Arc::clone(&self.pools),
            log: self.log,
            print: self.print,
        }
    }
}

pub struct StateCountersInstrumentation {
    connections: Arc<AtomicUsize>,
    idle: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    reservations: Arc<AtomicUsize>,
    pools: Arc<AtomicUsize>,
    log: bool,
    print: bool,
}

impl StateCountersInstrumentation {
    fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }
    fn idle(&self) -> usize {
        self.idle.load(Ordering::SeqCst)
    }
    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }
    fn reservations(&self) -> usize {
        self.reservations.load(Ordering::SeqCst)
    }
    fn pools(&self) -> usize {
        self.pools.load(Ordering::SeqCst)
    }

    fn output_required(&self) -> bool {
        self.log || self.print
    }

    fn output(&self, msg: &str) {
        if self.log {
            info!("{}", msg);
        }

        if self.print {
            let stdout = io::stdout();
            let mut handle = stdout.lock();
            let _ = handle.write_all(msg.as_bytes());
            let _ = handle.write_all(b"\n");
        }
    }
}

impl Instrumentation for StateCountersInstrumentation {
    fn pool_added(&self, pool: PoolId) {
        self.pools.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] pool added (+1): {}", pool, self.pools()));
        }
    }

    fn pool_removed(&self, pool: PoolId) {
        self.pools.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] pool removed (-1): {}", pool, self.pools()));
        }
    }

    fn checked_out_connection(
        &self,
        _idle_for: Duration,
        _time_since_checkout_request: Duration,
        pool: PoolId,
    ) {
        if self.output_required() {
            self.output(&format!("[{}] check out", pool));
        }
    }

    fn checked_in_returned_connection(&self, _flight_time: Duration, pool: PoolId) {
        if self.output_required() {
            self.output(&format!("[{}] check in returned", pool));
        }
    }

    fn checked_in_new_connection(&self, pool: PoolId) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{}] check in new connection (+1): {}",
                pool,
                self.connections()
            ));
        }
    }

    fn connection_dropped(
        &self,
        _flight_time: Option<Duration>,
        _lifetime: Duration,
        pool: PoolId,
    ) {
        self.connections.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{}] connection dropped (-1): {}",
                pool,
                self.connections()
            ));
        }
    }

    fn connection_created(&self, _connected_after: Duration, _total_time: Duration, _pool: PoolId) {
    }

    fn idle_inc(&self, pool: PoolId) {
        self.idle.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] idle +1: {}", pool, self.idle()));
        }
    }

    fn idle_dec(&self, pool: PoolId) {
        self.idle.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] idle -1: {}", pool, self.idle()));
        }
    }

    fn in_flight_inc(&self, pool: PoolId) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] in_flight +1: {}", pool, self.in_flight()));
        }
    }

    fn in_flight_dec(&self, pool: PoolId) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{}] in_flight -1: {}", pool, self.in_flight()));
        }
    }

    fn reservation_added(&self, pool: PoolId) {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{}] reservation added (+1): {}",
                pool,
                self.reservations()
            ));
        }
    }

    fn reservation_fulfilled(
        &self,
        _reservation_time: Duration,
        _checkout_request_time: Duration,
        pool: PoolId,
    ) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{}] reservation fulfilled (-1): {}",
                pool,
                self.reservations()
            ));
        }
    }

    fn reservation_not_fulfilled(
        &self,
        _reservation_time: Duration,
        _checkout_request_time: Duration,
        pool: PoolId,
    ) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{}] reservations not fulfilled (-1): {}",
                pool,
                self.reservations()
            ));
        }
    }

    fn reservation_limit_reached(&self, pool: PoolId) {
        if self.output_required() {
            self.output(&format!("[{}] reservation limit reached", pool));
        }
    }

    fn connection_factory_failed(&self, pool: PoolId) {
        if self.output_required() {
            self.output(&format!("[{}] connection factory failed", pool));
        }
    }

    fn internal_message_received(&self, _latency: Duration, _pool: PoolId) {}

    fn checkout_message_received(&self, _latency: Duration, _pool: PoolId) {}

    fn relevant_message_processed(&self, _processing_time: Duration, _pool: PoolId) {}
}
