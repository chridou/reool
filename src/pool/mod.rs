use std::error::Error as StdError;
use std::fmt;
use std::sync::{atomic::Ordering, Arc, Weak};
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    stream::Stream,
    sync::{mpsc, oneshot},
    Poll,
};
use log::{debug, trace, warn};
use tokio_timer::Delay;

use crate::backoff_strategy::BackoffStrategy;
use crate::error::ReoolError;
use crate::executor_flavour::*;

use inner_pool::InnerPool;

mod inner_pool;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub desired_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
    pub wait_queue_limit: Option<usize>,
}

#[cfg(test)]
impl Config {
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.desired_pool_size = v;
        self
    }

    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.backoff_strategy = v;
        self
    }

    /*pub fn wait_queue_limit(mut self, v: Option<usize>) -> Self {
        self.wait_queue_limit = v;
        self
    }*/
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            backoff_strategy: BackoffStrategy::default(),
            wait_queue_limit: Some(50),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub pool_size: usize,
    pub in_flight: usize,
    pub waiting: usize,
    pub idle: usize,
}

pub(crate) struct Pool<T: Poolable> {
    connection_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync>,
    inner_pool: Arc<InnerPool<T>>,
    new_con_tx: mpsc::UnboundedSender<NewConnMessage>,
    backoff_strategy: BackoffStrategy,
    executor: ExecutorFlavour,
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

        let num_connections = config.desired_pool_size;
        let inner_pool = Arc::new(InnerPool::new(config.clone(), new_con_tx.clone()));

        start_new_conn_stream(
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
            backoff_strategy: config.backoff_strategy,
            executor,
        };

        (0..num_connections).for_each(|_| pool.add_new_connection());

        pool
    }

    pub fn checkout(&self, timeout: Option<Duration>) -> Checkout<T> {
        self.inner_pool.checkout(timeout)
    }

    pub fn add_new_connection(&self) {
        let fut = self.create_new_poolable_conn().map(|_| ()).map_err(|err| {
            warn!("Failed to create initial connection: {}", err);
        });
        if let Err(err) = self.executor.execute(fut) {
            warn!("Failed to execute task for initial connection: {}", err);
        }
    }

    pub fn remove_connection(&self) {
        self.inner_pool.remove_conn()
    }

    pub fn stats(&self) -> PoolStats {
        self.inner_pool.stats()
    }

    fn create_new_poolable_conn(&self) -> NewConnFuture<NewConn<T>> {
        create_new_poolable_conn(
            Instant::now(),
            self.connection_factory.clone(),
            Arc::downgrade(&self.inner_pool),
            self.backoff_strategy,
            1,
        )
    }
}

impl<T: Poolable> Drop for Pool<T> {
    fn drop(&mut self) {
        let _ = self.new_con_tx.unbounded_send(NewConnMessage::Shutdown);
        debug!("pool dropped");
    }
}

fn create_new_poolable_conn<T>(
    initiated_at: Instant,
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
        let start_connect = Instant::now();
        let fut = connection_factory
            .create_connection()
            .then(move |res| match res {
                Ok(conn) => NewConnFuture::new(future::ok(NewConn {
                    total_time: initiated_at.elapsed(),
                    connect_time: start_connect.elapsed(),
                    managed: Managed {
                        value: Some(conn),
                        inner_pool: inner_pool,
                        marked_for_kill: false,
                        created_at: Instant::now(),
                        takeoff_at: None,
                    },
                })),
                Err(err) => {
                    inner_pool
                        .upgrade()
                        .into_iter()
                        .for_each(|p| p.notify_conn_factory_failed());
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
                                    initiated_at,
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
                            initiated_at,
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
                            Instant::now(),
                            connection_factory.clone(),
                            inner_pool,
                            back_off_strategy,
                            1,
                        )
                        .map(|_| ())
                        .map_err(|err| warn!("Failed to create new connection: {}", err));
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
    inner: Box<Future<Item = Managed<T>, Error = ReoolError> + Send + 'static>,
}

impl<T: Poolable> Checkout<T> {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Item = Managed<T>, Error = ReoolError> + Send + 'static,
    {
        Self {
            inner: Box::new(fut),
        }
    }
}

impl<T: Poolable> Future for Checkout<T> {
    type Item = Managed<T>;
    type Error = ReoolError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

pub trait Poolable: Send + Sized + 'static {}

pub struct Managed<T: Poolable> {
    created_at: Instant,
    takeoff_at: Option<Instant>,
    pub value: Option<T>,
    inner_pool: Weak<InnerPool<T>>,
    marked_for_kill: bool,
}

impl<T: Poolable> Managed<T> {}

impl<T: Poolable> Drop for Managed<T> {
    fn drop(&mut self) {
        if let Some(inner_pool) = self.inner_pool.upgrade() {
            if self.marked_for_kill {
                trace!("killed");
                inner_pool.notify_killed(self.created_at.elapsed());
            } else if let Some(value) = self.value.take() {
                inner_pool.put(Managed {
                    inner_pool: Arc::downgrade(&inner_pool),
                    value: Some(value),
                    marked_for_kill: false,
                    created_at: self.created_at,
                    takeoff_at: self.takeoff_at,
                });
            } else {
                trace!("no value - request new connection");
                // No connection. Create a new one.
                if let Some(takeoff_at) = self.takeoff_at {
                    inner_pool.notify_not_returned(takeoff_at.elapsed());
                } else {
                    inner_pool
                        .in_flight_connections
                        .fetch_sub(1, Ordering::SeqCst);
                    warn!("Returning connection without takeoff time. This is a BUG.");
                }
                inner_pool.request_new_conn();
            }
        } else {
            trace!("terminating connection because the pool is gone")
        }
    }
}

pub(crate) enum NewConnMessage {
    RequestNewConn,
    Shutdown,
}

pub(crate) struct NewConn<T: Poolable> {
    /// the time it took to connect to the backend on success
    connect_time: Duration,
    /// The time for the whole creation process including retries
    total_time: Duration,
    managed: Managed<T>,
}

impl<T: Poolable> Drop for NewConn<T> {
    fn drop(&mut self) {
        if let Some(inner_pool) = self.managed.inner_pool.upgrade() {
            inner_pool.notify_created(self.connect_time, self.total_time);
        } else {
            debug!("dropping new connection because pool is gone")
        }
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
