use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    stream::Stream,
    sync::{mpsc, oneshot},
    Poll,
};
use log::{debug, trace, warn};
use rand::prelude::*;
use tokio_timer::Delay;

use crate::error::Error;
use crate::executor_flavour::*;

mod inner_pool;

use inner_pool::InnerPool;

const NEW_CONN_BACKOFFS_MS: &[Duration] = &[
    Duration::from_millis(10),
    Duration::from_millis(20),
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(150),
    Duration::from_millis(200),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_millis(1_000),
    Duration::from_millis(2_500),
    Duration::from_millis(5_000),
    Duration::from_millis(7_500),
    Duration::from_millis(10_000),
    Duration::from_millis(15_000),
    Duration::from_millis(20_000),
    Duration::from_millis(25_000),
];
const MAX_NEW_CONN_BACKOFF_MS: Duration = Duration::from_millis(30_000);

#[derive(Debug, Clone, Copy)]
pub enum BackoffStrategy {
    /// Immediately retry
    NoBackoff,
    /// Retry always after a fixed interval. Maybe with some jitter.
    Constant { fixed: Duration, jitter: bool },
    /// Use incremental backoff. Max is 30s. Maybe with some jitter.
    Incremental { jitter: bool },
    /// Use incremental backoff but not more than `cap`. Max is 30s. Maybe with some jitter.
    /// The maximum is always 30s even if `cap` is greater.
    IncrementalCapped { cap: Duration, jitter: bool },
}

impl BackoffStrategy {
    pub(crate) fn get_next_backoff(&self, attempt: usize) -> Option<Duration> {
        fn calc_backoff(attempt: usize) -> Duration {
            let idx = (if attempt == 0 { 0 } else { attempt - 1 }) as usize;
            if idx < NEW_CONN_BACKOFFS_MS.len() {
                NEW_CONN_BACKOFFS_MS[idx]
            } else {
                MAX_NEW_CONN_BACKOFF_MS
            }
        }

        let (backoff, with_jitter) = match self {
            BackoffStrategy::NoBackoff => return None,
            BackoffStrategy::Constant { fixed, jitter } => (*fixed, *jitter),
            BackoffStrategy::Incremental { jitter } => (calc_backoff(attempt), *jitter),
            BackoffStrategy::IncrementalCapped { cap, jitter } => {
                let uncapped = calc_backoff(attempt);
                let effective = std::cmp::min(uncapped, *cap);
                (effective, *jitter)
            }
        };
        if with_jitter {
            let ms = backoff.as_millis() as u64;
            let twenty_percent = ms / 5;
            let effective_jitter = std::cmp::min(twenty_percent, 100);
            if effective_jitter >= 2 {
                let mut rng = rand::thread_rng();
                let jitter = rng.gen_range(0, effective_jitter);
                Some(backoff + Duration::from_millis(jitter))
            } else {
                Some(backoff)
            }
        } else {
            Some(backoff)
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
}

impl Config {
    pub fn max_pool_size(mut self, v: usize) -> Self {
        self.max_pool_size = v;
        self
    }

    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.backoff_strategy = v;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_pool_size: 20,
            backoff_strategy: BackoffStrategy::IncrementalCapped {
                cap: Duration::from_secs(5),
                jitter: true,
            },
        }
    }
}

struct Pool<T: Poolable> {
    connection_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync>,
    inner_pool: Arc<InnerPool<T>>,
    new_con_tx: mpsc::UnboundedSender<NewConnMessage>,
}

impl<T> Pool<T>
where
    T: Poolable,
{
    pub fn new<C>(config: Config, connection_factory: C, executor: ExecutorFlavour) -> Self
    where
        C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
    {
        let connection_factory = Arc::new(connection_factory);

        let (new_con_tx, new_conn_rx) = mpsc::unbounded();

        let num_connections = config.max_pool_size;
        let inner_pool = Arc::new(InnerPool::new(config.clone(), new_con_tx.clone()));

        let new_fut = start_new_conn_stream(
            new_conn_rx,
            connection_factory.clone(),
            Arc::downgrade(&inner_pool),
            &executor,
            config.backoff_strategy,
        );

        let pool = Self {
            connection_factory: connection_factory.clone(),
            inner_pool,
            new_con_tx,
        };

        (0..num_connections).for_each(|_| {
            let fut = pool
                .create_new_poolable_conn(config.backoff_strategy)
                .map(|_| ())
                .map_err(|err| {
                    warn!("Failed to create initial connection: {}", err);
                });
            if let Err(err) = executor.execute(fut) {
                warn!("Failed to execute task for initial connection: {}", err);
            }
        });

        pool
    }

    pub fn checkout(&self, timeout: Option<Duration>) -> Checkout<T> {
        self.inner_pool.checkout(timeout)
    }

    pub(crate) fn create_new_poolable_conn(
        &self,
        back_off_strategy: BackoffStrategy,
    ) -> NewConnFuture<NewConn<T>> {
        create_new_poolable_conn(
            self.connection_factory.clone(),
            Arc::downgrade(&self.inner_pool),
            back_off_strategy,
            1,
        )
    }

    pub fn usable_connections(&self) -> usize {
        self.inner_pool.usable_connections()
    }
}

impl<T: Poolable> Drop for Pool<T> {
    fn drop(&mut self) {
        self.new_con_tx.unbounded_send(NewConnMessage::Shutdown);
        debug!("pool dropped");
    }
}

fn create_new_poolable_conn<T>(
    connection_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>,
    inner_pool: Weak<InnerPool<T>>,
    back_off_strategy: BackoffStrategy,
    attempt: usize,
) -> NewConnFuture<NewConn<T>>
where
    T: Poolable,
{
    trace!("create new conn");
    if let Some(existing_inner_pool) = inner_pool.upgrade() {
        let inner_pool = Arc::downgrade(&existing_inner_pool);
        drop(existing_inner_pool);
        let fut = connection_factory
            .create_connection()
            .then(move |res| match res {
                Ok(conn) => NewConnFuture::new(future::ok(NewConn {
                    managed: Managed {
                        value: Some(conn),
                        inner_pool: inner_pool,
                    },
                })),
                Err(err) => {
                    if let Some(backoff) = back_off_strategy.get_next_backoff(attempt) {
                        let delay = Delay::new(Instant::now() + backoff);
                        warn!(
                            "Attempt {} to create a connection failed. Retry in {:?}. Error: {}",
                            attempt, backoff, err
                        );
                        let fut = delay
                            .map_err(|err| NewConnectionError::new(Box::new(err)))
                            .and_then(move |()| {
                                create_new_poolable_conn(
                                    connection_factory,
                                    inner_pool,
                                    back_off_strategy,
                                    attempt + 1,
                                )
                            });
                        NewConnFuture::new(fut)
                    } else {
                        warn!(
                        "Attempt {} to create a connection failed. Retry immediately. Error: {}",
                        attempt, err);
                        create_new_poolable_conn(
                            connection_factory,
                            inner_pool,
                            back_off_strategy,
                            attempt + 1,
                        )
                    }
                }
            });
        NewConnFuture::new(fut)
    } else {
        NewConnFuture::new(future::err(NewConnectionError::new(Box::new(
            PoolIsGoneError,
        ))))
    }
}

fn start_new_conn_stream<T, C>(
    receiver: mpsc::UnboundedReceiver<NewConnMessage>,
    connection_factory: Arc<C>,
    inner_pool: Weak<InnerPool<T>>,
    executor: &ExecutorFlavour,
    back_off_strategy: BackoffStrategy,
) where
    T: Poolable,
    C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
{
    let spawn_handle = executor.spawn_unbounded(receiver);

    let mut is_shut_down = false;
    let executor_a = executor.clone();
    let fut = spawn_handle.for_each(move |msg| {
        if is_shut_down {
            trace!("new conn requested on finished stream");
            Err(())
        } else {
            match msg {
                NewConnMessage::RequestNewConn => {
                    if let Some(existing_inner_pool) = inner_pool.upgrade() {
                        let inner_pool = Arc::downgrade(&existing_inner_pool);
                        drop(existing_inner_pool);
                        let fut = create_new_poolable_conn(
                            connection_factory.clone(),
                            inner_pool,
                            back_off_strategy,
                            1,
                        )
                        .map(|_| ())
                        .map_err(|err| {
                            warn!("Failed to create new connection: {}", err);
                        });
                        executor_a.execute(fut).map_err(|err| {
                            warn!("Failed to execute task for new connection: {}", err);
                            is_shut_down = true;
                            ()
                        })
                    } else {
                        Err(())
                    }
                }
                NewConnMessage::Shutdown => {
                    debug!("shutdown new conn stream");
                    is_shut_down = true;
                    Err(())
                }
            }
        }
    });

    executor.execute(fut).unwrap()
}

pub(crate) struct Checkout<T: Poolable> {
    inner: Box<Future<Item = CheckedOut<T>, Error = Error>>,
}

impl<T: Poolable> Checkout<T> {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Item = CheckedOut<T>, Error = Error> + 'static,
    {
        Self {
            inner: Box::new(fut),
        }
    }
}

impl<T: Poolable> Future for Checkout<T> {
    type Item = CheckedOut<T>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

pub trait Poolable: Send + Sized + 'static {}

pub struct Managed<T: Poolable> {
    pub value: Option<T>,
    inner_pool: Weak<InnerPool<T>>,
}

pub struct CheckedOut<T: Poolable> {
    pub managed: Managed<T>,
}

impl<T: Poolable> Drop for CheckedOut<T> {
    fn drop(&mut self) {
        if let Some(inner_pool) = self.managed.inner_pool.upgrade() {
            if let Some(value) = self.managed.value.take() {
                inner_pool.put(Managed {
                    inner_pool: Arc::downgrade(&inner_pool),
                    value: Some(value),
                });
            } else {
                // No managed connection. Create a new one.
                inner_pool.notify_not_returned();
                inner_pool.request_new_conn();
            }
        } else {
            trace!("dropping connection because the pool is gone")
        }
    }
}

pub enum NewConnMessage {
    RequestNewConn,
    Shutdown,
}

pub struct NewConn<T: Poolable> {
    managed: Managed<T>,
}

impl<T: Poolable> Drop for NewConn<T> {
    fn drop(&mut self) {
        if let Some(value) = self.managed.value.take() {
            if let Some(inner_pool) = self.managed.inner_pool.upgrade() {
                inner_pool.put(Managed {
                    inner_pool: Arc::downgrade(&inner_pool),
                    value: Some(value),
                });
                inner_pool.notify_created();
            } else {
                trace!("dropping new connection because pool is gone")
            }
        }
    }
}

struct Waiting<T: Poolable> {
    sender: oneshot::Sender<CheckedOut<T>>,
}

impl<T: Poolable> Waiting<T> {
    fn fulfill(self, checked_out: CheckedOut<T>) -> Option<CheckedOut<T>> {
        if let Err(checked_out) = self.sender.send(checked_out) {
            Some(checked_out)
        } else {
            None
        }
    }
}

pub(crate) struct NewConnFuture<T> {
    inner: Box<Future<Item = T, Error = NewConnectionError> + Send + 'static>,
}

impl<T> NewConnFuture<T> {
    pub fn new<F>(f: F) -> Self
    where
        F: Future<Item = T, Error = NewConnectionError> + Send + 'static,
    {
        Self { inner: Box::new(f) }
    }
}

impl<T> Future for NewConnFuture<T> {
    type Item = T;
    type Error = NewConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

pub(crate) struct NewConnectionError {
    cause: Box<StdError + Send + 'static>,
}

impl NewConnectionError {
    pub fn new<E>(cause: E) -> Self
    where
        E: StdError + Send + 'static,
    {
        Self {
            cause: Box::new(cause),
        }
    }
}

impl fmt::Display for NewConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "could not create a new connection: {}", self.cause)
    }
}

#[derive(Debug)]
struct PoolIsGoneError;

impl fmt::Display for PoolIsGoneError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str(self.description())
    }
}

impl StdError for PoolIsGoneError {
    fn description(&self) -> &str {
        "the pool was already gone"
    }

    fn cause(&self) -> Option<&StdError> {
        None
    }
}

pub(crate) trait ConnectionFactory {
    type Connection: Poolable;
    fn create_connection(&self) -> NewConnFuture<Self::Connection>;
}

#[cfg(test)]
mod test;
