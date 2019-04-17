use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace};
use parking_lot::Mutex;
use tokio_timer::Timeout;

use super::{CheckedOut, Checkout, Config, Managed, NewConnMessage, Poolable, Waiting};
use crate::error::{Error, ErrorKind};

pub(crate) struct InnerPool<T: Poolable> {
    usable_connections: AtomicUsize,
    idle: Mutex<VecDeque<Managed<T>>>,
    waiting: Mutex<VecDeque<Waiting<T>>>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(config: Config, request_new_conn: mpsc::UnboundedSender<NewConnMessage>) -> Self {
        let idle = VecDeque::with_capacity(config.max_pool_size);
        let waiting = VecDeque::with_capacity(config.max_pool_size);

        Self {
            usable_connections: AtomicUsize::new(0),
            idle: Mutex::new(idle),
            waiting: Mutex::new(waiting),
            request_new_conn,
        }
    }

    pub fn put(&self, managed: Managed<T>) {
        trace!("put managed");
        let mut all_waiting = self.waiting.lock();
        if all_waiting.is_empty() {
            self.idle.lock().push_back(managed);
        } else {
            let mut checked_out = CheckedOut { managed };
            while let Some(one_waiting) = all_waiting.pop_front() {
                if let Some(not_fulfilled) = one_waiting.fulfill(checked_out) {
                    checked_out = not_fulfilled;
                } else {
                    trace!("waiter fulfilled");
                    return;
                }
            }
        }
    }

    pub fn notify_not_returned(&self) {
        self.usable_connections.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn notify_created(&self) {
        self.usable_connections.fetch_add(1, Ordering::SeqCst);
    }

    pub fn usable_connections(&self) -> usize {
        self.usable_connections.load(Ordering::SeqCst)
    }

    pub fn checkout_idle(&self) -> Option<CheckedOut<T>> {
        self.idle
            .lock()
            .pop_front()
            .map(|managed| CheckedOut { managed })
    }

    pub fn checkout(&self, timeout: Option<Duration>) -> Checkout<T> {
        if let Some(checked_out) = self.checkout_idle() {
            Checkout::new(future::ok(checked_out))
        } else {
            let (tx, rx) = oneshot::channel();
            let waiting = Waiting { sender: tx };
            self.waiting.lock().push_back(waiting);
            trace!("waiter added");
            let fut = rx.map_err(|err| Error::with_cause(ErrorKind::NoConnection, err));
            if let Some(timeout) = timeout {
                let timeout_fut = Timeout::new(fut, timeout)
                    .map_err(|err| Error::with_cause(ErrorKind::NoConnection, err));
                Checkout::new(timeout_fut)
            } else {
                Checkout::new(fut)
            }
        }
    }

    pub fn request_new_conn(&self) {
        if let Err(_) = self
            .request_new_conn
            .unbounded_send(NewConnMessage::RequestNewConn)
        {
            error!("could not request a new connection")
        }
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        debug!("inner pool dropped");
    }
}
