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

/// Used to ensure there is no race between choeckouts and puts
struct SyncCore<T: Poolable> {
    pub idle: Vec<Managed<T>>,
    pub waiting: VecDeque<Waiting<T>>,
}

pub(crate) struct InnerPool<T: Poolable> {
    core: Mutex<SyncCore<T>>,
    pool_size: AtomicUsize,
    in_flight_connections: AtomicUsize,
    waiting_for_checkout: AtomicUsize,
    idle_connections: AtomicUsize,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(config: Config, request_new_conn: mpsc::UnboundedSender<NewConnMessage>) -> Self {
        let idle = Vec::with_capacity(config.desired_pool_size);
        let waiting = VecDeque::new();

        let core = Mutex::new(SyncCore { idle, waiting });

        Self {
            core,
            pool_size: AtomicUsize::new(0),
            in_flight_connections: AtomicUsize::new(0),
            waiting_for_checkout: AtomicUsize::new(0),
            idle_connections: AtomicUsize::new(0),
            request_new_conn,
            config,
        }
    }

    pub(super) fn put(&self, managed: Managed<T>) {
        trace!("put");
        // Do not let any Managed get dropped in here
        // because all_waiting might get locked twice!

        if let Some(takeoff_at) = managed.takeoff_at {
            self.notify_conn_returned(takeoff_at.elapsed());
        } else {
            self.notify_new_connection();
        }

        let mut core = self.core.lock();

        if core.waiting.is_empty() {
            core.idle.push(managed);
            trace!("put - added to idle");
            self.notify_idle_conns(core.idle.len());
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.waiting.pop_front() {
                if let Some(not_fulfilled) = one_waiting.fulfill(to_fulfill, self) {
                    to_fulfill = not_fulfilled;
                } else {
                    self.notify_waiting_queue_length(core.waiting.len());
                    return;
                }
            }
            core.idle.push(to_fulfill);
            self.notify_idle_conns(core.idle.len());
        }
    }

    pub(super) fn checkout(&self, timeout: Option<Duration>) -> Checkout<T> {
        trace!("checkout");

        let mut core = self.core.lock();

        if let Some(mut managed) = {
            let taken = core.idle.pop();
            self.notify_idle_conns(core.idle.len());
            taken
        } {
            managed.takeoff_at = Some(Instant::now());
            self.notify_takeoff();
            Checkout::new(future::ok(managed.into()))
        } else {
            if let Some(wait_queue_limit) = self.config.wait_queue_limit {
                if core.waiting.len() > wait_queue_limit {
                    trace!("wait queue limit reached - no connection");
                    self.notify_queue_limit_reached();
                    return Checkout::new(future::err(ReoolError::new(
                        ErrorKind::QueueLimitReached,
                    )));
                }
                trace!("no idle connection to checkout - enqueue for checkout");
            }
            let (tx, rx) = oneshot::channel();
            let waiting = Waiting::checkout(tx);
            core.waiting.push_back(waiting);
            self.notify_waiting();
            self.notify_waiting_queue_length(core.waiting.len());

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
            core.waiting.push_back(Waiting::reduce_pool_size());
        }
    }

    pub(super) fn notify_takeoff(&self) {
        self.in_flight_connections.fetch_add(1, Ordering::SeqCst);
    }

    fn notify_conn_returned(&self, _flight_time: Duration) {
        self.in_flight_connections.fetch_sub(1, Ordering::SeqCst);
    }

    pub(super) fn notify_not_returned(&self, _flight_time: Duration) {
        self.pool_size.fetch_sub(1, Ordering::SeqCst);
        self.in_flight_connections.fetch_sub(1, Ordering::SeqCst);
    }

    fn notify_idle_conns(&self, v: usize) {
        self.idle_connections.store(v, Ordering::SeqCst);
    }

    fn notify_new_connection(&self) {
        self.pool_size.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn notify_created(&self, _connected_after: Duration, _total_time: Duration) {}

    pub(super) fn notify_killed(&self, _lifetime: Duration) {
        self.pool_size.fetch_sub(1, Ordering::SeqCst);
    }

    fn notify_waiting_queue_length(&self, len: usize) {
        self.waiting_for_checkout.store(len, Ordering::SeqCst);
    }

    fn notify_waiting(&self) {}

    pub(super) fn notify_fulfilled(&self, _after: Duration) {}

    pub(super) fn notify_not_fulfilled(&self, _after: Duration) {}

    pub(super) fn notify_conn_factory_failed(&self) {}

    fn notify_queue_limit_reached(&self) {}

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

enum Waiting<T: Poolable> {
    Checkout(oneshot::Sender<Managed<T>>, Instant),
    ReducePoolSize,
}

impl<T: Poolable> Waiting<T> {
    pub fn checkout(sender: oneshot::Sender<Managed<T>>) -> Self {
        Waiting::Checkout(sender, Instant::now())
    }

    pub fn reduce_pool_size() -> Self {
        Waiting::ReducePoolSize
    }
}

impl<T: Poolable> Waiting<T> {
    fn fulfill(self, mut managed: Managed<T>, inner_pool: &InnerPool<T>) -> Option<Managed<T>> {
        managed.takeoff_at = Some(Instant::now());
        match self {
            Waiting::Checkout(sender, waiting_since) => {
                if let Err(mut managed) = sender.send(managed) {
                    trace!("not fulfilled");
                    inner_pool.notify_not_fulfilled(waiting_since.elapsed());
                    managed.takeoff_at = None;
                    Some(managed)
                } else {
                    trace!("fulfilled");
                    inner_pool.notify_takeoff();
                    inner_pool.notify_fulfilled(waiting_since.elapsed());
                    None
                }
            }
            Waiting::ReducePoolSize => {
                managed.takeoff_at = None;
                managed.marked_for_kill = true;
                None
            }
        }
    }
}
