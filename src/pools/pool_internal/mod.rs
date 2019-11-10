use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use futures::{
    future::{Future, FutureExt},
    stream::{StreamExt, TryStreamExt},
    channel::mpsc,
};
use log::{debug, trace, warn};
use tokio::{self, timer::delay};

use crate::activation_order::ActivationOrder;
use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::{ConnectionFactory, NewConnectionError};
use crate::error::CheckoutError;
use crate::executor_flavour::*;
use crate::instrumentation::InstrumentationFlavour;
use crate::pooled_connection::ConnectionFlavour;
use crate::{Ping, Poolable};

use inner_pool::{CheckInParcel, InnerPool};

mod inner_pool;
pub(crate) mod instrumentation;

use self::instrumentation::PoolInstrumentation;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub desired_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
    pub reservation_limit: Option<usize>,
    pub stats_interval: Duration,
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
            stats_interval: Duration::from_millis(100),
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
            PoolInstrumentation::new(InstrumentationFlavour::NoInstrumentation, 0),
        )
    }

    pub fn check_out(&self, timeout: Option<Duration>) -> impl Future<Output = Result<Managed<T>, CheckoutError>> + '_ {
        self.inner_pool.check_out(timeout)
    }

    #[cfg(test)]
    #[allow(unused)]
    fn inner_pool(&self) -> &Arc<InnerPool<T>> {
        &self.inner_pool
    }

    pub fn connected_to(&self) -> &[String] {
        self.inner_pool.connected_to()
    }

    fn add_new_connection(&self) {
        trace!("add new connection request");
        self.inner_pool.request_new_conn();
    }
}

impl PoolInternal<ConnectionFlavour> {
    pub fn ping(&self, timeout: Duration) -> impl Future<Output = Ping> + '_ {
        self.inner_pool.ping(timeout)
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
    let fut = spawn_handle.map(Ok).try_for_each(move |msg| {
        let connection_factory = Arc::clone(&connection_factory);
        let inner_pool = Weak::clone(&inner_pool);

        async move {
            if is_shut_down {
                trace!("new connection requested on finished stream");
                return Err(());
            }

            match msg {
                NewConnMessage::RequestNewConn => {
                    if let Some(existing_inner_pool) = inner_pool.upgrade() {
                        trace!("creating new connection");

                        create_new_managed(
                            Instant::now(),
                            connection_factory.clone(),
                            Arc::downgrade(&existing_inner_pool),
                            back_off_strategy,
                        )
                        .await
                        .map_err(|_| ())?;

                        Ok(())
                    } else {
                        warn!("attempt to create new connection even though the pool is gone");
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
    })
    .map(|_| ());

    executor.execute(fut).unwrap()
}

impl<T: Poolable> Clone for PoolInternal<T> {
    fn clone(&self) -> Self {
        Self {
            inner_pool: self.inner_pool.clone(),
        }
    }
}

async fn create_new_managed<T: Poolable, C>(
    initiated_at: Instant,
    connection_factory: Arc<C>,
    weak_inner_pool: Weak<InnerPool<T>>,
    back_off_strategy: BackoffStrategy,
) -> Result<Managed<T>, NewConnectionError>
where
    T: Poolable,
    C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
{
    let mut attempt = 1;

    loop {
        let inner_pool = weak_inner_pool.upgrade()
            .ok_or_else(|| NewConnectionError::new(Box::new(
                PoolIsGoneError,
            )))?;

        let start_connect = Instant::now();

        match connection_factory.create_connection().await {
            Ok(conn) => {
                trace!("new connection created");

                inner_pool.notify_connection_created(
                    initiated_at.elapsed(),
                    start_connect.elapsed(),
                );

                break Ok(Managed::fresh(
                    conn,
                    Arc::downgrade(&inner_pool),
                ));
            }
            Err(err) => {
                inner_pool.notify_connection_factory_failed();

                if let Some(backoff) = back_off_strategy.get_next_backoff(attempt) {
                    warn!(
                        "Attempt {} to create a connection failed. Retry in {:?}. Error: {}",
                        attempt, backoff, err
                    );

                    delay(Instant::now() + backoff).await;

                    continue;
                } else {
                    warn!(
                        "Attempt {} to create a connection failed. Retry immediately. Error: {}",
                        attempt, err
                    );

                    attempt += 1;

                    continue;
                }
            }
        }
    }
}

pub(crate) struct Managed<T: Poolable> {
    created_at: Instant,
    /// Is `Some` taken from the pool otherwise fresh connection
    checked_out_at: Option<Instant>,
    pub value: Option<T>,
    inner_pool: Weak<InnerPool<T>>,
}

impl<T: Poolable> Managed<T> {
    pub fn fresh(value: T, inner_pool: Weak<InnerPool<T>>) -> Self {
        Managed {
            value: Some(value),
            inner_pool,
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
        let inner_pool = match self.inner_pool.upgrade() {
            Some(inner_pool) => inner_pool,
            None => {
                trace!("terminating connection because the pool is gone");
                return;
            }
        };

        if let Some(value) = self.value.take() {
            let managed = Managed {
                inner_pool: Arc::downgrade(&inner_pool),
                value: Some(value),
                created_at: self.created_at,
                checked_out_at: self.checked_out_at,
            };
            let parcel = CheckInParcel::Alive(managed);

            tokio::spawn(async move {
                inner_pool.check_in(parcel).await
            });
        } else {
            // No connection. Create a new one.
            debug!("no value - drop connection and request new one");
            let parcel = CheckInParcel::Dropped(
                self.checked_out_at.as_ref().map(Instant::elapsed),
                self.created_at.elapsed(),
            );

            inner_pool.request_new_conn();

            tokio::spawn(async move {
                inner_pool.check_in(parcel).await;
            });
        }
    }
}

pub(crate) enum NewConnMessage {
    RequestNewConn,
    Shutdown,
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
