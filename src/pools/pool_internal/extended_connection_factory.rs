//! A pimped connection factory
use std::sync::Arc;
use std::time::Instant;

use futures::prelude::*;
use future::BoxFuture;
use log::{trace, warn};
use tokio::sync::mpsc;
use tokio::time::sleep;

use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::ConnectionFactory;
use crate::{Ping, Poolable};

use super::inner_pool::PoolMessage;

use super::instrumentation::PoolInstrumentation;
use super::{Managed, PoolMessageEnvelope};

/// A connection factory that uses a retry logic when creating connections. As long
/// as the pool is there, it will retry to create a connection.
///
/// Once a connection is created it will be send to the pool via a channel.
///
/// There is also instrumentation included since some metrics can be directly sent
/// after a connection was created or not created.
pub(crate) struct ExtendedConnectionFactory<T: Poolable> {
    inner_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>,
    send_back: mpsc::UnboundedSender<PoolMessageEnvelope<T>>,
    pub instrumentation: PoolInstrumentation,
    back_off_strategy: BackoffStrategy,
}

impl<T: Poolable> ExtendedConnectionFactory<T> {
    pub fn new(
        inner_factory: Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>,
        send_back: mpsc::UnboundedSender<PoolMessageEnvelope<T>>,
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
    pub fn send_back_cloned(&self) -> mpsc::UnboundedSender<PoolMessageEnvelope<T>> {
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
    pub fn create_connection(mut self, initiated_at: Instant) {
        let f = async move {
            let mut attempt = 1;

            let result = loop {
                // Probe the channel to the inner pool
                if self
                    .send_message(PoolMessage::CheckAlive(Instant::now()))
                    .is_err()
                {
                    break Err("Pool is gone.".to_string());
                }

                match self.do_a_create_connection_attempt(initiated_at).await {
                    Ok(managed) => {
                        drop(managed); // We send it to the pool by dropping it

                        trace!("Dropped newly created connection to be sent to pool");

                        break Ok(());
                    }
                    Err(this) => {
                        self = this;

                        // break delayed_by_backoff_strategy(&factory, &mut attempt).await
                        if let Some(backoff) = self.back_off_strategy.get_next_backoff(attempt) {
                            warn!(
                                "Retry on in to create connection after attempt {} in {:?}",
                                attempt, backoff
                            );

                            sleep(backoff).await;
                        } else {
                            warn!(
                                "Retry on in to create connection after attempt {} immediately",
                                attempt
                            );
                        }

                        attempt += 1;
                    },
                }
            };

            if let Err(err) = result {
                warn!("Create connection finally failed: {}", err);
            }
        };

        tokio::spawn(f);
    }

    pub fn connecting_to(&self) -> &str {
        self.inner_factory.connecting_to()
    }

    pub fn ping(&self, timeout: Instant) -> BoxFuture<Ping> {
        self.inner_factory.ping(timeout)
    }

    /// Do one attempt on the inner connection factory to
    /// get a new connection
    fn do_a_create_connection_attempt(
        self,
        initiated_at: Instant,
    ) -> impl Future<Output = Result<Managed<T>, Self>> {
        let start_connect = Instant::now();
        let inner_factory = Arc::clone(&self.inner_factory);

        async move {
            let conn = inner_factory.create_connection().await;

            match conn {
                Ok(conn) => {
                    trace!("new connection created");

                    self.instrumentation
                        .connection_created(initiated_at.elapsed(), start_connect.elapsed());

                    Ok(Managed::fresh(conn, self))
                }
                Err(err) => {
                    self.instrumentation.connection_factory_failed();

                    warn!("Connection factory failed: {}", err);

                    Err(self)
                }
            }
        }
    }
}
