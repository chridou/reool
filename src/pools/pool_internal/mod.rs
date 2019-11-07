use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future, Loop},
    stream::Stream,
    sync::mpsc,
    Poll,
};
use log::{debug, trace, warn};
use tokio_timer::{Delay, Timeout};

use crate::activation_order::ActivationOrder;
use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::{ConnectionFactory, NewConnectionError};
use crate::error::CheckoutError;
use crate::executor_flavour::*;
use crate::instrumentation::InstrumentationFlavour;
use crate::pooled_connection::ConnectionFlavour;
use crate::{Ping, PingState, Poolable};

use inner_pool::{CheckInParcel, InnerPool};

mod inner_pool;
pub(crate) mod instrumentation;

use instrumentation::PoolInstrumentation;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub desired_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
    pub reservation_limit: Option<usize>,
    pub activation_order: ActivationOrder,
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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: Some(50),
            activation_order: ActivationOrder::default(),
        }
    }
}

pub(crate) struct PoolInternal<T: Poolable> {
    inner_pool: Arc<InnerPool<T>>,
}

impl<T> PoolInternal<T>
where
    T: Poolable,
{
    pub fn new<C>(
        config: Config,
        connection_factory: C,
        executor: ExecutorFlavour,
        instrumentation: PoolInstrumentation,
    ) -> Self
    where
        C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
    {
        let (new_con_tx, new_conn_rx) = mpsc::unbounded();

        let num_connections = config.desired_pool_size;
        let inner_pool = Arc::new(InnerPool::new(
            connection_factory
                .connecting_to()
                .iter()
                .map(|c| c.as_ref().to_owned())
                .collect(),
            config.clone(),
            new_con_tx.clone(),
            instrumentation,
        ));

        start_new_conn_stream(
            new_conn_rx,
            Arc::new(connection_factory),
            Arc::downgrade(&inner_pool),
            executor,
            config.backoff_strategy,
        );

        let pool = Self { inner_pool };

        (0..num_connections).for_each(|_| {
            pool.add_new_connection();
        });

        pool
    }

    #[cfg(test)]
    pub fn no_instrumentation<C>(
        config: Config,
        connection_factory: C,
        executor: ExecutorFlavour,
    ) -> Self
    where
        C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
    {
        Self::new(
            config,
            connection_factory,
            executor,
            PoolInstrumentation {
                pool_index: 0,
                flavour: InstrumentationFlavour::NoInstrumentation,
            },
        )
    }

    pub fn check_out(&self, timeout: Option<Duration>) -> CheckoutManaged<T> {
        self.inner_pool.check_out(timeout)
    }

    pub fn add_new_connection(&self) {
        trace!("add new connection request");
        self.inner_pool.request_new_conn();
    }

    pub fn remove_connection(&self) {
        self.inner_pool.remove_conn()
    }

    pub fn connected_to(&self) -> &[String] {
        self.inner_pool.connected_to()
    }

    #[cfg(test)]
    #[allow(unused)]
    fn inner_pool(&self) -> &Arc<InnerPool<T>> {
        &self.inner_pool
    }
}

impl PoolInternal<ConnectionFlavour> {
    pub fn ping(&self, timeout: Duration) -> impl Future<Item = Ping, Error = ()> + Send {
        use crate::commands::Commands;

        #[derive(Debug)]
        struct PingError(String);

        impl fmt::Display for PingError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl StdError for PingError {
            fn description(&self) -> &str {
                "ping failed"
            }

            fn cause(&self) -> Option<&dyn StdError> {
                None
            }
        }

        let started_at = Instant::now();

        let known_connected_to = if self.inner_pool.connected_to().len() == 1 {
            Some(self.inner_pool.connected_to()[0].to_owned())
        } else {
            None
        };

        let f = crate::Checkout(self.inner_pool.check_out(Some(timeout)))
            .map_err(|err| (Box::new(err) as Box<dyn StdError + Send>, None))
            .and_then(|conn| {
                let connected_to = conn.connected_to().to_owned();
                conn.ping().then(|r| match r {
                    Ok(_) => Ok(connected_to),
                    Err(err) => Err((
                        Box::new(err) as Box<dyn StdError + Send>,
                        Some(connected_to),
                    )),
                })
            });

        Timeout::new(f, timeout).then(move |r| {
            let (uri, state) = match r {
                Ok(uri) => (Some(uri), PingState::Ok),
                Err(err) => {
                    if err.is_inner() {
                        let (err, uri) = err.into_inner().unwrap();
                        (uri, PingState::Failed(err))
                    } else if err.is_elapsed() {
                        (
                            known_connected_to,
                            PingState::Failed(Box::new(PingError(format!(
                                "ping time out of {:?} reached",
                                timeout
                            )))),
                        )
                    } else {
                        (
                            known_connected_to,
                            PingState::Failed(Box::new(PingError(
                                "a timer error occurred".to_string(),
                            ))),
                        )
                    }
                }
            };

            Ok(Ping {
                uri,
                latency: started_at.elapsed(),
                state,
            })
        })
    }
}

fn start_new_conn_stream<T, C>(
    receiver: mpsc::UnboundedReceiver<NewConnMessage>,
    connection_factory: Arc<C>,
    inner_pool: Weak<InnerPool<T>>,
    executor: ExecutorFlavour,
    back_off_strategy: BackoffStrategy,
) where
    T: Poolable,
    C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
{
    let spawn_handle = executor.spawn_unbounded(receiver);

    let mut is_shut_down = false;
    let fut = spawn_handle.for_each(move |msg| {
        if is_shut_down {
            trace!("new connection requested on finished stream");
            Box::new(future::err(()))
        } else {
            match msg {
                NewConnMessage::RequestNewConn => {
                    if let Some(existing_inner_pool) = inner_pool.upgrade() {
                        trace!("creating new connection");
                        let fut = create_new_managed(
                            Instant::now(),
                            connection_factory.clone(),
                            Arc::downgrade(&existing_inner_pool),
                            back_off_strategy,
                        )
                        .map(|_| ())
                        .map_err(|err| warn!("Failed to create new connection: {}", err));
                        drop(existing_inner_pool);
                        Box::new(fut)
                    } else {
                        warn!("attempt to create new connection even though the pool is gone");
                        Box::new(future::err(())) as Box<dyn Future<Item = _, Error = _> + Send>
                    }
                }
                NewConnMessage::Shutdown => {
                    debug!("shutdown new conn stream");
                    is_shut_down = true;
                    Box::new(future::err(()))
                }
            }
        }
    });

    executor.execute(fut).unwrap()
}

impl<T: Poolable> Clone for PoolInternal<T> {
    fn clone(&self) -> Self {
        Self {
            inner_pool: self.inner_pool.clone(),
        }
    }
}

fn create_new_managed<T: Poolable, C>(
    initiated_at: Instant,
    connection_factory: Arc<C>,
    weak_inner_pool: Weak<InnerPool<T>>,
    back_off_strategy: BackoffStrategy,
) -> NewManaged<T>
where
    T: Poolable,
    C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
{
    let fut = future::loop_fn((weak_inner_pool, 1), move |(weak_inner, attempt)| {
        if let Some(inner_pool) = weak_inner.upgrade() {
            let start_connect = Instant::now();
            let fut = connection_factory
                .create_connection()
                .then(move |res| match res {
                    Ok(conn) => {
                        trace!("new connection created");
                        inner_pool.notify_connection_created(
                            initiated_at.elapsed(),
                            start_connect.elapsed(),
                        );
                        Box::new(future::ok(Loop::Break(Managed::fresh(
                            conn,
                            Arc::downgrade(&inner_pool),
                        ))))
                    }
                    Err(err) => {
                        inner_pool.notify_connection_factory_failed();
                        if let Some(backoff) = back_off_strategy.get_next_backoff(attempt) {
                            let delay = Delay::new(Instant::now() + backoff);
                            warn!(
                            "Attempt {} to create a connection failed. Retry in {:?}. Error: {}",
                            attempt, backoff, err
                        );
                            Box::new(
                                delay
                                    .map_err(|err| NewConnectionError::new(Box::new(err)))
                                    .and_then(move |()| {
                                        future::ok(Loop::Continue((
                                            Arc::downgrade(&inner_pool),
                                            attempt + 1,
                                        )))
                                    }),
                            )
                                as Box<dyn Future<Item = _, Error = _> + Send>
                        } else {
                            warn!(
                        "Attempt {} to create a connection failed. Retry immediately. Error: {}",
                        attempt, err);
                            Box::new(future::ok(Loop::Continue((
                                Arc::downgrade(&inner_pool),
                                attempt + 1,
                            ))))
                        }
                    }
                });
            Box::new(fut) as Box<dyn Future<Item = _, Error = _> + Send>
        } else {
            Box::new(future::err(NewConnectionError::new(Box::new(
                PoolIsGoneError,
            ))))
        }
    });
    NewManaged::new(fut)
}

pub(crate) struct CheckoutManaged<T: Poolable> {
    inner: Box<dyn Future<Item = Managed<T>, Error = CheckoutError> + Send + 'static>,
}

impl<T: Poolable> CheckoutManaged<T> {
    pub fn new<F>(fut: F) -> Self
    where
        F: Future<Item = Managed<T>, Error = CheckoutError> + Send + 'static,
    {
        Self {
            inner: Box::new(fut),
        }
    }
}

impl<T: Poolable> Future for CheckoutManaged<T> {
    type Item = Managed<T>;
    type Error = CheckoutError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
    }
}

pub(crate) struct Managed<T: Poolable> {
    created_at: Instant,
    /// Is `Some` taken from the pool otherwise fresh connection
    checked_out_at: Option<Instant>,
    pub value: Option<T>,
    inner_pool: Weak<InnerPool<T>>,
    marked_for_kill: bool,
}

impl<T: Poolable> Managed<T> {
    pub fn fresh(value: T, inner_pool: Weak<InnerPool<T>>) -> Self {
        Managed {
            value: Some(value),
            inner_pool,
            marked_for_kill: false,
            created_at: Instant::now(),
            checked_out_at: None,
        }
    }

    pub fn connected_to(&self) -> &str {
        self.value
            .as_ref()
            .expect("no value in managed - this is a bug")
            .connected_to()
    }
}

impl<T: Poolable> Drop for Managed<T> {
    fn drop(&mut self) {
        if let Some(inner_pool) = self.inner_pool.upgrade() {
            if self.marked_for_kill {
                inner_pool.check_in(CheckInParcel::Killed(self.created_at.elapsed()))
            } else if let Some(value) = self.value.take() {
                inner_pool.check_in(CheckInParcel::Alive(Managed {
                    inner_pool: Arc::downgrade(&inner_pool),
                    value: Some(value),
                    marked_for_kill: false,
                    created_at: self.created_at,
                    checked_out_at: self.checked_out_at,
                }));
            } else {
                debug!("no value - drop connection and request new one");
                // No connection. Create a new one.
                inner_pool.check_in(CheckInParcel::Dropped(
                    self.checked_out_at.as_ref().map(Instant::elapsed),
                    self.created_at.elapsed(),
                ));
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

pub(crate) struct NewManaged<T: Poolable> {
    inner: Box<dyn Future<Item = Managed<T>, Error = NewConnectionError> + Send + 'static>,
}

impl<T: Poolable> NewManaged<T> {
    pub fn new<F>(f: F) -> Self
    where
        F: Future<Item = Managed<T>, Error = NewConnectionError> + Send + 'static,
    {
        Self { inner: Box::new(f) }
    }
}

impl<T: Poolable> Future for NewManaged<T> {
    type Item = Managed<T>;
    type Error = NewConnectionError;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        self.inner.poll()
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

    fn cause(&self) -> Option<&dyn StdError> {
        None
    }
}

#[cfg(test)]
mod test;
