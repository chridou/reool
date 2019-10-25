use std::collections::VecDeque;
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace};
use parking_lot::{Mutex, MutexGuard};
use tokio_timer::Timeout;

use super::{CheckoutManaged, Config, Managed, NewConnMessage};
use crate::activation_order::ActivationOrder;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::instrumentation::Instrumentation;
use crate::{
    stats::{MinMax, PoolStats},
    Poolable,
};

pub(crate) struct InnerPool<T: Poolable> {
    core: Mutex<SyncCore<T>>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
    instrumentation: Option<Box<dyn Instrumentation + Send + Sync>>,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new<I>(
        config: Config,
        request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
        instrumentation: Option<I>,
    ) -> Self
    where
        I: Instrumentation + Send + Sync + 'static,
    {
        let core = Mutex::new(SyncCore::new(
            config.desired_pool_size,
            config.stats_interval,
            config.activation_order,
        ));

        Self {
            core,
            request_new_conn,
            config,
            instrumentation: instrumentation
                .map(|i| Box::new(i) as Box<dyn Instrumentation + Send + Sync>),
        }
    }

    pub(super) fn check_in(&self, parcel: CheckInParcel<T>) {
        match parcel {
            CheckInParcel::Alive(managed) => self.check_in_alive(managed),
            CheckInParcel::Dropped(in_flight_time, life_time) => {
                let mut core = self.core.lock();
                core.pool_size_tracker.dec();
                core.in_flight_tracker.dec();
                unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
                if let Some((instrumentation, flight_time)) = self
                    .instrumentation
                    .as_ref()
                    .and_then(|i| in_flight_time.map(|ft| (i, ft)))
                {
                    instrumentation.connection_dropped(flight_time, life_time);
                }
            }
            CheckInParcel::Killed(life_time) => {
                let mut core = self.core.lock();
                core.pool_size_tracker.dec();
                debug!(
                    "connection killed - pool size {}",
                    core.pool_size_tracker.current()
                );
                unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
                if let Some(instrumentation) = self.instrumentation.as_ref() {
                    instrumentation.connection_killed(life_time);
                }
            }
        }
    }

    fn check_in_alive(&self, mut managed: Managed<T>) {
        let checked_out_at = managed.checked_out_at.take();

        if let Some(instrumentation) = self.instrumentation.as_ref() {
            if let Some(checked_out_at) = checked_out_at {
                instrumentation.checked_in_returned_connection(checked_out_at.elapsed());
            } else {
                instrumentation.checked_in_new_connection();
            }
        }

        let mut core = self.core.lock();

        if checked_out_at.is_none() {
            core.pool_size_tracker.inc();
            trace!(
                "check in - new connection - pool size {}",
                core.pool_size_tracker.current()
            );
        } else {
            core.in_flight_tracker.dec();
            trace!(
                "check in - returning connection - in flight {}",
                core.in_flight_tracker.current()
            );
        }

        if core.reservations.is_empty() {
            core.idle.put(managed);
            core.idle_tracker.inc();
            trace!(
                "check in - no reservations - added to idle {}",
                core.idle_tracker.current()
            );
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.reservations.pop_front() {
                core.reservations_tracker.dec();
                match one_waiting.try_fulfill(to_fulfill) {
                    Fulfillment::Reservation(waited_for) => {
                        core.in_flight_tracker.inc();
                        trace!(
                            "fulfill reservation - fulfilled - in-flight {}",
                            core.in_flight_tracker.current()
                        );

                        unlock_then_publish_stats(
                            core,
                            self.instrumentation.as_ref().map(|i| &**i),
                        );

                        if let Some(instrumentation) = self.instrumentation.as_ref() {
                            instrumentation.checked_out_connection(Duration::from_secs(0));
                            instrumentation.reservation_fulfilled(waited_for);
                        }

                        return;
                    }
                    Fulfillment::Killed => {
                        core.pool_size_tracker.dec();
                        trace!(
                            "reservation - killed - pool size {}",
                            core.pool_size_tracker.current()
                        );

                        unlock_then_publish_stats(
                            core,
                            self.instrumentation.as_ref().map(|i| &**i),
                        );
                        return;
                    }
                    Fulfillment::NotFulfilled(not_fulfilled, waited_for) => {
                        trace!("fulfill reservation - not fulfilled");
                        if let Some(instrumentation) = self.instrumentation.as_ref() {
                            instrumentation.reservation_not_fulfilled(waited_for)
                        }

                        to_fulfill = not_fulfilled;
                    }
                }
            }

            core.idle.put(to_fulfill);
            let idle_count = core.idle.len();
            let reservations_count = core.reservations.len();
            core.idle_tracker.set(idle_count);
            core.reservations_tracker.set(reservations_count);
            trace!(
                "check in - none fulfilled - added to idle {}",
                core.idle_tracker.current()
            );
        }

        unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
    }

    pub(super) fn check_out(&self, timeout: Option<Duration>) -> CheckoutManaged<T> {
        let mut core = self.core.lock();

        if let Some((mut managed, idle_since)) = { core.idle.get() } {
            trace!("check out - checking out idle connection");
            managed.checked_out_at = Some(Instant::now());
            core.idle_tracker.dec();
            core.in_flight_tracker.inc();

            unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));

            if let Some(instrumentation) = self.instrumentation.as_ref() {
                instrumentation.checked_out_connection(idle_since);
            }

            CheckoutManaged::new(future::ok(managed))
        } else {
            if let Some(reservation_limit) = self.config.reservation_limit {
                if core.reservations.len() > reservation_limit {
                    trace!(
                        "check out - reservation limit reached \
                         - returning error"
                    );

                    unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));

                    if let Some(instrumentation) = self.instrumentation.as_ref() {
                        instrumentation.reservation_limit_reached()
                    }

                    return CheckoutManaged::new(future::err(CheckoutError::new(
                        CheckoutErrorKind::QueueLimitReached,
                    )));
                }
            }
            trace!(
                "check out - no idle connection - \
                 enqueue reservation"
            );
            Self::create_reservation(timeout, core, self.instrumentation.as_ref().map(|i| &**i))
        }
    }

    fn create_reservation(
        timeout: Option<Duration>,
        mut core: MutexGuard<SyncCore<T>>,
        instrumentation: Option<&(dyn Instrumentation + Send + Sync)>,
    ) -> CheckoutManaged<T> {
        let (tx, rx) = oneshot::channel();
        let waiting = Reservation::checkout(tx);
        core.reservations.push_back(waiting);
        core.reservations_tracker.inc();

        let fut = rx
            .map(From::from)
            .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::NoConnection, err));
        let fut = if let Some(timeout) = timeout {
            let timeout_fut = Timeout::new(fut, timeout)
                .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::CheckoutTimeout, err));
            CheckoutManaged::new(timeout_fut)
        } else {
            CheckoutManaged::new(fut)
        };

        unlock_then_publish_stats(core, instrumentation);

        if let Some(instrumentation) = instrumentation {
            instrumentation.reservation_added()
        }

        fut
    }

    pub(super) fn request_new_conn(&self) {
        if self
            .request_new_conn
            .unbounded_send(NewConnMessage::RequestNewConn)
            .is_err()
        {
            error!("could not request a new connection")
        }
    }

    pub(super) fn remove_conn(&self) {
        let mut core = self.core.lock();
        if let Some((mut managed, _)) = { core.idle.get() } {
            core.idle_tracker.dec();
            drop(core);
            managed.marked_for_kill = true;
        } else {
            trace!("no idle connection to kill - enqueue for kill");
            core.reservations.push_back(Reservation::reduce_pool_size());
            core.reservations_tracker.inc();
            unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
            if let Some(instrumentation) = self.instrumentation.as_ref() {
                instrumentation.reservation_added()
            }
        }
    }

    // ==== Instrumentation ====

    #[inline]
    pub(super) fn notify_connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
    ) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.connection_created(connected_after, total_time)
        }
    }

    #[inline]
    pub(super) fn notify_connection_factory_failed(&self) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.connection_factory_failed()
        }
    }

    pub fn stats(&self) -> PoolStats {
        self.core.lock().stats()
    }

    pub fn trigger_stats(&self) {
        unlock_then_publish_stats(
            self.core.lock(),
            self.instrumentation.as_ref().map(|i| &**i),
        );
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        let _ = self
            .request_new_conn
            .unbounded_send(NewConnMessage::Shutdown);

        if let Some(instr) = self.instrumentation.as_ref() {
            instr.stats(PoolStats::default())
        }

        debug!("inner pool dropped - all connections will be terminated on return");
    }
}

// ===== CHECK IN PARCEL =====

pub(super) enum CheckInParcel<T: Poolable> {
    Alive(Managed<T>),
    Dropped(Option<Duration>, Duration),
    Killed(Duration),
}

// ===== SYNC CORE =====

/// Used to ensure there is no race between checkouts and puts
struct SyncCore<T: Poolable> {
    pub idle: IdleConnections<Managed<T>>,
    pub reservations: VecDeque<Reservation<T>>,
    pub idle_tracker: ValueTracker,
    pub in_flight_tracker: ValueTracker,
    pub reservations_tracker: ValueTracker,
    pub pool_size_tracker: ValueTracker,
    pub stats_interval: Duration,
    pub last_flushed: Instant,
}

impl<T: Poolable> SyncCore<T> {
    fn new(
        desired_pool_size: usize,
        stats_interval: Duration,
        activation_order: ActivationOrder,
    ) -> Self {
        Self {
            idle: IdleConnections::new(desired_pool_size, activation_order),
            reservations: VecDeque::default(),
            idle_tracker: ValueTracker::default(),
            in_flight_tracker: ValueTracker::default(),
            reservations_tracker: ValueTracker::default(),
            pool_size_tracker: ValueTracker::default(),
            stats_interval,
            last_flushed: Instant::now() - stats_interval,
        }
    }
}

impl<T: Poolable> SyncCore<T> {
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            pool_size: self.pool_size_tracker.get(),
            in_flight: self.in_flight_tracker.get(),
            reservations: self.reservations_tracker.get(),
            idle: self.idle_tracker.get(),
            node_count: 1,
        }
    }

    pub fn try_flush(&mut self) -> Option<PoolStats> {
        let now = Instant::now();
        if self.last_flushed + self.stats_interval >= now {
            None
        } else {
            self.last_flushed = now;
            let current = self.stats();

            self.pool_size_tracker.apply_flush();
            self.in_flight_tracker.apply_flush();
            self.reservations_tracker.apply_flush();
            self.idle_tracker.apply_flush();

            Some(current)
        }
    }
}

// ===== RESERVATION =====

enum Reservation<T: Poolable> {
    Checkout(oneshot::Sender<Managed<T>>, Instant),
    ReducePoolSize,
}

enum Fulfillment<T: Poolable> {
    NotFulfilled(Managed<T>, Duration),
    Reservation(Duration),
    Killed,
}

impl<T: Poolable> Reservation<T> {
    pub fn checkout(sender: oneshot::Sender<Managed<T>>) -> Self {
        Reservation::Checkout(sender, Instant::now())
    }

    pub fn reduce_pool_size() -> Self {
        Reservation::ReducePoolSize
    }
}

impl<T: Poolable> Reservation<T> {
    fn try_fulfill(self, mut managed: Managed<T>) -> Fulfillment<T> {
        match self {
            Reservation::Checkout(sender, waiting_since) => {
                managed.checked_out_at = Some(Instant::now());
                if let Err(mut managed) = sender.send(managed) {
                    managed.checked_out_at = None;
                    Fulfillment::NotFulfilled(managed, waiting_since.elapsed())
                } else {
                    Fulfillment::Reservation(waiting_since.elapsed())
                }
            }
            Reservation::ReducePoolSize => {
                managed.checked_out_at = None;
                managed.marked_for_kill = true;
                Fulfillment::Killed
            }
        }
    }
}

// ===== VALUE TRACKER ======

struct ValueTracker {
    last: usize,
    min_max: Option<MinMax>,
}

impl Default for ValueTracker {
    fn default() -> Self {
        Self {
            last: 0,
            min_max: None,
        }
    }
}

impl ValueTracker {
    #[inline]
    fn set(&mut self, v: usize) {
        self.last = v;
        let new_min_max = if let Some(mut min_max) = self.min_max.take() {
            if v < min_max.0 {
                min_max.0 = v;
            }
            if v > min_max.1 {
                min_max.1 = v;
            }
            min_max
        } else {
            MinMax(v, v)
        };
        self.min_max = Some(new_min_max)
    }

    pub fn inc(&mut self) {
        self.set(self.last + 1);
    }

    pub fn dec(&mut self) {
        self.set(self.last - 1);
    }

    #[inline]
    fn get(&self) -> MinMax {
        if let Some(min_max) = self.min_max.as_ref() {
            *min_max
        } else {
            MinMax(self.last, self.last)
        }
    }

    #[inline]
    fn current(&self) -> usize {
        self.last
    }

    fn apply_flush(&mut self) {
        self.min_max = None;
    }
}

fn unlock_then_publish_stats<T: Poolable>(
    mut core_guard: MutexGuard<SyncCore<T>>,
    instrumentation: Option<&(dyn Instrumentation + Send + Sync)>,
) {
    let snapshot = core_guard.try_flush();
    drop(core_guard);
    if let (Some(instr), Some(snapshot)) = (instrumentation, snapshot) {
        instr.stats(snapshot)
    }
}

struct IdleSlot<T>(T, Instant);

enum IdleConnections<T> {
    FiFo(VecDeque<IdleSlot<T>>),
    LiFo(Vec<IdleSlot<T>>),
}

impl<T> IdleConnections<T> {
    pub fn new(size: usize, activation_order: ActivationOrder) -> Self {
        match activation_order {
            ActivationOrder::FiFo => IdleConnections::FiFo(VecDeque::with_capacity(size)),
            ActivationOrder::LiFo => IdleConnections::LiFo(Vec::with_capacity(size)),
        }
    }

    #[inline]
    pub fn put(&mut self, conn: T) {
        match self {
            IdleConnections::FiFo(idle) => idle.push_back(IdleSlot(conn, Instant::now())),
            IdleConnections::LiFo(idle) => idle.push(IdleSlot(conn, Instant::now())),
        }
    }

    #[inline]
    pub fn get(&mut self) -> Option<(T, Duration)> {
        match self {
            IdleConnections::FiFo(idle) => idle.pop_front(),
            IdleConnections::LiFo(idle) => idle.pop(),
        }
        .map(|IdleSlot(conn, idle_since)| (conn, idle_since.elapsed()))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            IdleConnections::FiFo(idle) => idle.len(),
            IdleConnections::LiFo(idle) => idle.len(),
        }
    }
}
