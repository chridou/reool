//! A pimped connection factory
use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future, Loop};
use log::{trace, warn};
use tokio::sync::mpsc;
use tokio::{self, timer::Delay};

use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::ConnectionFactory;
use crate::{Ping, Poolable};

use super::inner_pool::PoolMessage;

use super::instrumentation::PoolInstrumentation;
use super::{Managed, MessageWrapper};

/// A connection factory that uses a retry logic when creating connections. As long
/// as the pool is there, it will retry to create a connection.
///
/// Once a connection is created it will be send to the pool via a channel.
///
/// There is also instrumentation included since some metrics can be directly sent
/// after a connection was created or not created.
pub(crate) struct ExtendedConnectionFactory<T: Poolable> {
    inner_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>,
    send_back: mpsc::UnboundedSender<MessageWrapper<T>>,
    pub instrumentation: PoolInstrumentation,
    back_off_strategy: BackoffStrategy,
}

impl<T: Poolable> ExtendedConnectionFactory<T> {
    pub fn new(
        inner_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>,
        send_back: mpsc::UnboundedSender<MessageWrapper<T>>,
        instrumentation: PoolInstrumentation,
        back_off_strategy: BackoffStrategy,
    ) -> Self {
        Self {
            inner_factory,
            send_back,
            instrumentation,
            back_off_strategy,
        }
    }

    /// Returns a cloned version of the sender to the internal channel
    pub fn send_back_cloned(&self) -> mpsc::UnboundedSender<MessageWrapper<T>> {
        self.send_back.clone()
    }

    /// Sends a `PoolMessage` on the internal channel.
    ///
    /// The message will be wrapped automatically and being unwrapped when
    /// the message could not be sent.
    pub fn send_message(&mut self, message: PoolMessage<T>) -> Result<(), PoolMessage<T>> {
        message.send_on_internal_channel(&mut self.send_back)
    }

    /// Create a new connections and try as long as the pool is there.
    ///
    /// Before each attempt this functions tries to send a probing message to the pool. If
    /// sending the message fails the channel to the pool is disconnected which
    /// means that the pool has been dropped.
    pub fn create_connection(self, initiated_at: Instant) {
        let f = future::loop_fn((self, 1), move |(mut factory, attempt)| {
            // Probe the channel to the inner pool
            if factory
                .send_message(PoolMessage::CheckAlive(Instant::now()))
                .is_err()
            {
                Box::new(future::err("Pool is gone.".to_string()))
            } else {
                Box::new(factory.do_a_create_connection_attempt(initiated_at).then(
                    move |r| match r {
                        Ok(managed) => {
                            drop(managed); // We send it to the pool by dropping it
                            trace!("Dropped newly created connection to be sent to pool");
                            Box::new(future::ok(Loop::Break(())))
                        }
                        Err(factory) => Box::new(delayed_by_backoff_strategy(factory, attempt))
                            as Box<dyn Future<Item = _, Error = String> + Send>,
                    },
                )) as Box<dyn Future<Item = _, Error = String> + Send>
            }
        })
        .then(|r| {
            if let Err(err) = r {
                warn!("Create connection finally failed: {}", err);
            }
            Ok(())
        });

        tokio::spawn(f);
    }

    pub fn connecting_to(&self) -> &str {
        self.inner_factory.connecting_to()
    }

    pub fn ping(&self, timeout: Instant) -> impl Future<Item = Ping, Error = ()> + Send {
        self.inner_factory.ping(timeout)
    }

    /// Do one attempt on the inner connection factory to
    /// get a new connection
    fn do_a_create_connection_attempt(
        self,
        initiated_at: Instant,
    ) -> impl Future<Item = Managed<T>, Error = Self> + Send {
        let start_connect = Instant::now();
        let inner_factory = Arc::clone(&self.inner_factory);
        inner_factory
            .create_connection()
            .then(move |res| match res {
                Ok(conn) => {
                    trace!("new connection created");
                    self.instrumentation
                        .connection_created(initiated_at.elapsed(), start_connect.elapsed());
                    future::ok(Managed::fresh(conn, self))
                }
                Err(err) => {
                    self.instrumentation.connection_factory_failed();
                    warn!("Connection factory failed: {}", err);
                    future::err(self)
                }
            })
    }
}

/// Applies a delay based on the backoff strategy. If there is no
/// backoff we retry immediately.
fn delayed_by_backoff_strategy<T: Poolable>(
    factory: ExtendedConnectionFactory<T>,
    attempt: usize,
) -> impl Future<Item = Loop<(), (ExtendedConnectionFactory<T>, usize)>, Error = String> + Send {
    if let Some(backoff) = factory.back_off_strategy.get_next_backoff(attempt) {
        let delay = Delay::new(Instant::now() + backoff);
        warn!(
            "Retry on in to create connection after attempt {} in {:?}",
            attempt, backoff
        );
        Box::new(
            delay
                .map_err(|err| err.to_string())
                .and_then(move |()| future::ok(Loop::Continue((factory, attempt + 1)))),
        ) as Box<dyn Future<Item = _, Error = _> + Send>
    } else {
        warn!(
            "Retry on in to create connection after attempt {} immediately",
            attempt
        );
        Box::new(future::ok(Loop::Continue((factory, attempt + 1))))
    }
}
