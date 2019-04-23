use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace};
use parking_lot::Mutex;
use tokio_timer::Timeout;

use super::{Checkout, Config, Managed, NewConnMessage, PoolStats, Poolable};
use crate::error::{ErrorKind, ReoolError};
use crate::instrumentation::Instrumentation;

/// Used to ensure there is no race between choeckouts and puts
struct SyncCore<T: Poolable> {
    pub idle: Vec<Managed<T>>,
    pub reservations: VecDeque<Reservation<T>>,
}

pub(crate) struct InnerPool<T: Poolable> {
    core: Mutex<SyncCore<T>>,
    pool_size: AtomicUsize,
    in_flight_connections: AtomicUsize,
    waiting_for_checkout: AtomicUsize,
    idle_connections: AtomicUsize,
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
        let idle = Vec::with_capacity(config.desired_pool_size);
        let reservations = VecDeque::new();

        let core = Mutex::new(SyncCore { idle, reservations });

        Self {
            core,
            pool_size: AtomicUsize::new(0),
            in_flight_connections: AtomicUsize::new(0),
            waiting_for_checkout: AtomicUsize::new(0),
            idle_connections: AtomicUsize::new(0),
            request_new_conn,
            config,
            instrumentation: instrumentation
                .map(|i| Box::new(i) as Box<dyn Instrumentation + Send + Sync>),
        }
    }

    pub(super) fn check_in(&self, managed: Managed<T>) {
        trace!("check in");
        // Do not let any Managed get dropped in here
        // because core might get locked twice!

        if let Some(checked_out_at) = managed.checked_out_at {
            self.notify_checked_in_returned_connection(checked_out_at.elapsed());
        } else {
            trace!("check in - new connection");
            self.notify_checked_in_new_connection();
        }

        let mut core = self.core.lock();

        if core.reservations.is_empty() {
            core.idle.push(managed);
            trace!("check in - no reservations - added to idle");
            self.notify_idle_connections_changed(core.idle.len());
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.reservations.pop_front() {
                if let Some(not_fulfilled) = one_waiting.fulfill(to_fulfill, self) {
                    to_fulfill = not_fulfilled;
                } else {
                    self.notify_reservations_changed(core.reservations.len());
                    return;
                }
            }
            core.idle.push(to_fulfill);
            trace!("check in - none fulfilled - added to idle");
            self.notify_idle_connections_changed(core.idle.len());
            self.notify_reservations_changed(core.reservations.len());
        }
    }

    pub(super) fn check_out(&self, timeout: Option<Duration>) -> Checkout<T> {
        trace!("check out");

        let mut core = self.core.lock();

        if let Some(mut managed) = {
            let taken = core.idle.pop();
            self.notify_idle_connections_changed(core.idle.len());
            taken
        } {
            managed.checked_out_at = Some(Instant::now());
            self.notify_checked_out_connection();
            Checkout::new(future::ok(managed.into()))
        } else {
            if let Some(reservation_limit) = self.config.reservation_limit {
                if core.reservations.len() > reservation_limit {
                    trace!(
                        "check out - reservation limit reached \
                         - returning error"
                    );
                    self.notify_reservation_limit_reached();
                    return Checkout::new(future::err(ReoolError::new(
                        ErrorKind::QueueLimitReached,
                    )));
                }
                trace!(
                    "check out - no idle connection - \
                     enqueue reservation"
                );
            }
            let (tx, rx) = oneshot::channel();
            let waiting = Reservation::checkout(tx);
            core.reservations.push_back(waiting);
            self.notify_reservation_added();
            self.notify_reservations_changed(core.reservations.len());

            let fut = rx
                .map(From::from)
                .map_err(|err| ReoolError::with_cause(ErrorKind::NoConnection, err));
            if let Some(timeout) = timeout {
                let timeout_fut = Timeout::new(fut, timeout)
                    .map_err(|err| ReoolError::with_cause(ErrorKind::Timeout, err));
                Checkout::new(timeout_fut)
            } else {
                Checkout::new(fut)
            }
        }
    }

    pub(super) fn request_new_conn(&self) {
        if let Err(_) = self
            .request_new_conn
            .unbounded_send(NewConnMessage::RequestNewConn)
        {
            error!("could not request a new connection")
        }
    }

    pub(super) fn remove_conn(&self) {
        let mut core = self.core.lock();
        if let Some(mut managed) = { core.idle.pop() } {
            managed.marked_for_kill = true;
        } else {
            trace!("no idle connection to kill - enqueue for kill");
            core.reservations.push_back(Reservation::reduce_pool_size());
        }
    }

    // ==== Instrumentation ====

    #[inline]
    pub(super) fn notify_checked_out_connection(&self) {
        self.in_flight_connections.fetch_add(1, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.checked_out_connection()
        }
    }

    #[inline]
    fn notify_checked_in_returned_connection(&self, flight_time: Duration) {
        self.in_flight_connections.fetch_sub(1, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.checked_in_returned_connection(flight_time)
        }
    }

    #[inline]
    fn notify_checked_in_new_connection(&self) {
        self.pool_size.fetch_add(1, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.checked_in_new_connection()
        }
    }

    #[inline]
    pub(super) fn notify_dropped_connection(&self, flight_time: Duration) {
        self.pool_size.fetch_sub(1, Ordering::SeqCst);
        self.in_flight_connections.fetch_sub(1, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.dropped_connection(flight_time)
        }
    }

    #[inline]
    fn notify_idle_connections_changed(&self, len: usize) {
        self.idle_connections.store(len, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.idle_connections_changed(len)
        }
    }

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
    pub(super) fn notify_killed_connection(&self, lifetime: Duration) {
        self.pool_size.fetch_sub(1, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.killed_connection(lifetime)
        }
    }

    #[inline]
    fn notify_reservations_changed(&self, len: usize) {
        self.waiting_for_checkout.store(len, Ordering::SeqCst);
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.reservations_changed(len)
        }
    }

    #[inline]
    fn notify_reservation_added(&self) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.reservation_added()
        }
    }

    #[inline]
    pub(super) fn notify_reservation_fulfilled(&self, after: Duration) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.reservation_fulfilled(after)
        }
    }

    #[inline]
    pub(super) fn notify_reservation_not_fulfilled(&self, after: Duration) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.reservation_not_fulfilled(after)
        }
    }

    #[inline]
    fn notify_reservation_limit_reached(&self) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.reservation_limit_reached()
        }
    }

    #[inline]
    pub(super) fn notify_connection_factory_failed(&self) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.connection_factory_failed()
        }
    }

    pub fn stats(&self) -> PoolStats {
        PoolStats {
            pool_size: self.pool_size.load(Ordering::SeqCst),
            in_flight: self.in_flight_connections.load(Ordering::SeqCst),
            waiting: self.waiting_for_checkout.load(Ordering::SeqCst),
            idle: self.idle_connections.load(Ordering::SeqCst),
        }
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        debug!("inner pool dropped");
    }
}

enum Reservation<T: Poolable> {
    Checkout(oneshot::Sender<Managed<T>>, Instant),
    ReducePoolSize,
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
    fn fulfill(self, mut managed: Managed<T>, inner_pool: &InnerPool<T>) -> Option<Managed<T>> {
        managed.checked_out_at = Some(Instant::now());
        match self {
            Reservation::Checkout(sender, waiting_since) => {
                if let Err(mut managed) = sender.send(managed) {
                    trace!("fulfill reservation - not fulfilled");
                    inner_pool.notify_reservation_not_fulfilled(waiting_since.elapsed());
                    managed.checked_out_at = None;
                    Some(managed)
                } else {
                    trace!("fulfill reservation - fulfilled");
                    inner_pool.notify_checked_out_connection();
                    inner_pool.notify_reservation_fulfilled(waiting_since.elapsed());
                    None
                }
            }
            Reservation::ReducePoolSize => {
                trace!("reservation - mark for kill");
                managed.checked_out_at = None;
                managed.marked_for_kill = true;
                None
            }
        }
    }
}
