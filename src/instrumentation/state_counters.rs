use std::io::{self, Write};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use log::info;

use super::Instrumentation;

/// Simply tracks the following values:
///
/// * number of existing connections
/// * number of idle connections
/// * number of in flight connections
/// * number of reservations
/// * then number of tasks waiting before the lock
///
/// This is mostly useful for testing purposes only.
#[derive(Clone, Default)]
pub struct StateCounters {
    connections: Arc<AtomicUsize>,
    idle: Arc<AtomicUsize>,
    in_flight: Arc<AtomicUsize>,
    reservations: Arc<AtomicUsize>,
    pools: Arc<AtomicUsize>,
    contentions: Arc<AtomicUsize>,
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
    pub fn contentions(&self) -> usize {
        self.contentions.load(Ordering::SeqCst)
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

impl Instrumentation for StateCounters {
    fn pool_added(&self, pool_index: usize) {
        self.pools.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] pool added (+1): {}",
                pool_index,
                self.pools()
            ));
        }
    }

    fn pool_removed(&self, pool_index: usize) {
        self.pools.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] pool removed (-1): {}",
                pool_index,
                self.pools()
            ));
        }
    }

    fn checked_out_connection(&self, _idle_for: Duration, pool_index: usize) {
        if self.output_required() {
            self.output(&format!("[{:02}] check out", pool_index));
        }
    }

    fn checked_in_returned_connection(&self, _flight_time: Duration, pool_index: usize) {
        if self.output_required() {
            self.output(&format!("[{:02}] check in returned", pool_index));
        }
    }

    fn checked_in_new_connection(&self, pool_index: usize) {
        self.connections.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] check in new connection (+1): {}",
                pool_index,
                self.connections()
            ));
        }
    }

    fn connection_dropped(
        &self,
        _flight_time: Option<Duration>,
        _lifetime: Duration,
        pool_index: usize,
    ) {
        self.connections.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] connection dropped (-1): {}",
                pool_index,
                self.connections()
            ));
        }
    }

    fn connection_created(
        &self,
        _connected_after: Duration,
        _total_time: Duration,
        _pool_index: usize,
    ) {
    }

    fn idle_inc(&self, pool_index: usize) {
        self.idle.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{:02}] idle +1: {}", pool_index, self.idle()));
        }
    }

    fn idle_dec(&self, pool_index: usize) {
        self.idle.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!("[{:02}] idle -1: {}", pool_index, self.idle()));
        }
    }

    fn in_flight_inc(&self, pool_index: usize) {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] in_flight +1: {}",
                pool_index,
                self.in_flight()
            ));
        }
    }

    fn in_flight_dec(&self, pool_index: usize) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] in_flight -1: {}",
                pool_index,
                self.in_flight()
            ));
        }
    }

    fn reservation_added(&self, pool_index: usize) {
        self.reservations.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] reservation added (+1): {}",
                pool_index,
                self.reservations()
            ));
        }
    }

    fn reservation_fulfilled(&self, _after: Duration, pool_index: usize) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] reservation fulfilled (-1): {}",
                pool_index,
                self.reservations()
            ));
        }
    }

    fn reservation_not_fulfilled(&self, _after: Duration, pool_index: usize) {
        self.reservations.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] reservations not fulfilled (-1): {}",
                pool_index,
                self.reservations()
            ));
        }
    }

    fn reservation_limit_reached(&self, pool_index: usize) {
        if self.output_required() {
            self.output(&format!("[{}] reservation limit reached", pool_index));
        }
    }

    fn connection_factory_failed(&self, pool_index: usize) {
        if self.output_required() {
            self.output(&format!("[{}] connection factory failed", pool_index));
        }
    }

    fn reached_lock(&self, pool_index: usize) {
        self.contentions.fetch_add(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] contention +1: {}",
                pool_index,
                self.contentions()
            ));
        }
    }

    fn passed_lock(&self, _wait_time: Duration, pool_index: usize) {
        self.contentions.fetch_sub(1, Ordering::SeqCst);
        if self.output_required() {
            self.output(&format!(
                "[{:02}] contention -1: {}",
                pool_index,
                self.contentions()
            ));
        }
    }

    fn lock_released(&self, _exclusive_lock_time: Duration, _pool_index: usize) {}
}
