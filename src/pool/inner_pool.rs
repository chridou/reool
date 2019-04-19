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

use super::{Checkout, Config, Managed, NewConnMessage, PoolStats, Poolable, Waiting};
use crate::error::{Error, ErrorKind};

pub(crate) struct InnerPool<T: Poolable> {
    pool_size: AtomicUsize,
    pub(super) in_flight_connections: AtomicUsize,
    waiting_for_checkout: AtomicUsize,
    idle_connections: AtomicUsize,
    idle: Mutex<VecDeque<Managed<T>>>,
    waiting: Mutex<VecDeque<Waiting<T>>>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(config: Config, request_new_conn: mpsc::UnboundedSender<NewConnMessage>) -> Self {
        let idle = VecDeque::with_capacity(config.desired_pool_size);
        let waiting = VecDeque::new();

        Self {
            pool_size: AtomicUsize::new(0),
            in_flight_connections: AtomicUsize::new(0),
            waiting_for_checkout: AtomicUsize::new(0),
            idle_connections: AtomicUsize::new(0),
            idle: Mutex::new(idle),
            waiting: Mutex::new(waiting),
            request_new_conn,
            config,
        }
    }

    pub(super) fn put(&self, managed: Managed<T>) {
        trace!("put");
        if let Some(takeoff_at) = managed.takeoff_at {
            self.notify_returned(takeoff_at.elapsed());
        } else {
            self.notify_new_connection();
        }
        let mut all_waiting = self.waiting.lock();
        if all_waiting.is_empty() {
            let mut idle = self.idle.lock();
            idle.push_back(managed);
            self.notify_idle_conns(idle.len());
        } else {
            let mut to_fulfill = managed;
            while let Some(one_waiting) = all_waiting.pop_front() {
                if let Some(not_fulfilled) = one_waiting.fulfill(to_fulfill, self) {
                    to_fulfill = not_fulfilled;
                } else {
                    self.notify_waiting_queue_length(all_waiting.len());
                    return;
                }
            }
        }
    }

    pub(super) fn checkout(&self, timeout: Option<Duration>) -> Checkout<T> {
        if let Some(mut managed) = {
            let mut idle = self.idle.lock();
            let taken = idle.pop_front();
            self.notify_idle_conns(idle.len());
            taken
        } {
            trace!("checkout idle");
            managed.takeoff_at = Some(Instant::now());
            self.notify_takeoff();
            Checkout::new(future::ok(managed.into()))
        } else {
            let rx = {
                let mut all_waiting = self.waiting.lock();
                if let Some(wait_queue_limit) = self.config.wait_queue_limit {
                    if all_waiting.len() > wait_queue_limit {
                        trace!("wait queue limit reached - no connection");
                        self.notify_queue_limit_reached();
                        return Checkout::new(future::err(Error::new(ErrorKind::NoConnection)));
                    }
                    trace!("no immediate connection - enqueue for checkout");
                }
                let (tx, rx) = oneshot::channel();
                let waiting = Waiting::checkout(tx);
                all_waiting.push_back(waiting);
                self.notify_waiting();
                self.notify_waiting_queue_length(all_waiting.len());
                rx
            };
            let fut = rx
                .map(From::from)
                .map_err(|err| Error::with_cause(ErrorKind::NoConnection, err));
            if let Some(timeout) = timeout {
                let timeout_fut = Timeout::new(fut, timeout)
                    .map_err(|err| Error::with_cause(ErrorKind::NoConnection, err));
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
        if let Some(mut managed) = { self.idle.lock().pop_front() } {
            managed.marked_for_kill = true;
        } else {
            trace!("no immediate connection - enqueue for kill");
            self.waiting.lock().push_back(Waiting::reduce_pool_size());
        }
    }

    pub(super) fn notify_takeoff(&self) {
        self.in_flight_connections.fetch_add(1, Ordering::SeqCst);
    }

    fn notify_returned(&self, _flight_time: Duration) {
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
