use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use futures::prelude::*;
use future::BoxFuture;
use log::trace;
use tokio::sync::{mpsc, oneshot};
use tokio::sync::mpsc::error::TrySendError;
use tokio::time;

use crate::activation_order::ActivationOrder;
use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::ConnectionFactory;
use crate::error::CheckoutErrorKind;
use crate::executor_flavour::*;
#[cfg(test)]
use crate::instrumentation::{InstrumentationFlavour, PoolId};
use crate::PoolState;
use crate::{Ping, Poolable};

use super::CheckoutConstraint;
use inner_pool::{CheckoutPayload, InnerPool, PoolMessage};

mod extended_connection_factory;
mod inner_pool;
pub(crate) mod instrumentation;
mod managed;

use self::extended_connection_factory::ExtendedConnectionFactory;
use self::instrumentation::PoolInstrumentation;
pub(crate) use self::managed::Managed;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub desired_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
    pub reservation_limit: usize,
    pub activation_order: ActivationOrder,
    pub checkout_queue_size: usize,
}

/// A wrapper for a pool message so that we can also send a
/// stop message over the stream without the need of the inner pool
/// to know about this message
pub(crate) enum PoolMessageEnvelope<T: Poolable> {
    /// Pass the content of this to the inner pool
    PoolMessage(PoolMessage<T>),
    /// Do not pass anything to the inner pool but stop
    /// the stream
    Stop,
}

pub(crate) struct CheckoutRequest<T: Poolable> {
    pub created_at: Instant,
    pub payload: CheckoutPayload<T>,
}

impl<T: Poolable> PoolMessage<T> {
    /// Wrap the message, send it on the channel and in case of a failure
    /// return the original message
    fn send_on_internal_channel(
        self,
        channel: &mut mpsc::UnboundedSender<PoolMessageEnvelope<T>>,
    ) -> Result<(), PoolMessage<T>> {
        let wrapped = PoolMessageEnvelope::PoolMessage(self);
        channel.send(wrapped).map_err(|err| {
            if let PoolMessageEnvelope::PoolMessage(msg) = err.0 {
                msg
            } else {
                panic!("Did not send a PoolMessage - THIS IS A BUG");
            }
        })
    }
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

    pub fn reservation_limit(mut self, v: usize) -> Self {
        self.reservation_limit = v;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: 100,
            activation_order: ActivationOrder::default(),
            checkout_queue_size: 100,
        }
    }
}

pub(crate) struct PoolInternal<T: Poolable> {
    extended_connection_factory: Arc<ExtendedConnectionFactory<T>>,
    checkout_sink: mpsc::Sender<CheckoutRequest<T>>,
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
        // We want to send inner messages in an unbounded manner.
        let (internal_tx, internal_receiver) =
            mpsc::unbounded_channel::<PoolMessageEnvelope<T>>();
        // Checkout messages should be capped so that we do not get flooded.
        let (checkout_sink, checkout_receiver) =
            mpsc::channel::<CheckoutRequest<T>>(config.checkout_queue_size);

        let inner_pool = InnerPool::new(&config, instrumentation.clone());
        start_inner_pool_consumer(inner_pool, checkout_receiver, internal_receiver, &executor);

        // We need to access it from multiple places since we are
        // going to put it into multiple `ExtendedConnectionFactory`s
        let wrapped_connection_factory = Arc::new(connection_factory)
            as Arc<dyn ConnectionFactory<Connection = T> + Send + Sync + 'static>;

        // Create the initial connections
        (0..config.desired_pool_size).for_each(|_| {
            // One for each connection
            let extended_connection_factory = ExtendedConnectionFactory::new(
                Arc::clone(&wrapped_connection_factory),
                internal_tx.clone(),
                instrumentation.clone(),
                config.backoff_strategy,
            );

            // This triggers the creation of a connection.
            // Once these connections fail they will recreate themselves
            let _ = executor.spawn(async {
                extended_connection_factory.create_connection(Instant::now());
            });
        });

        // We keep one for this pool to access it.
        let extended_connection_factory = Arc::new(ExtendedConnectionFactory::new(
            Arc::clone(&wrapped_connection_factory),
            internal_tx.clone(),
            instrumentation,
            config.backoff_strategy,
        ));

        // A loop driven by an interval to send periodic messages to the inner pool.
        // Since this stream tries to send to the pool stream it will
        // end once it fails to send a message to the pool stream.
        let cleanup_ticker = async move {
            let mut interval = time::interval(self::inner_pool::CLEANUP_INTERVAL);

            loop {
                interval.tick().await;

                let wrapped = PoolMessageEnvelope::PoolMessage(PoolMessage::CleanupReservations(
                    Instant::now(),
                ));

                if let Err(_err) = internal_tx.send(wrapped) {
                    trace!("pool gone - cleanup ticker stopping");
                    break
                }
            }
        };

        let _ = executor.spawn(cleanup_ticker);

        trace!("PoolInternal created");

        // The counterpart is triggered in `Self::drop`.
        extended_connection_factory.instrumentation.pool_added();

        Self {
            extended_connection_factory,
            checkout_sink,
        }
    }

    /// Try to send a message to the inner pool to check out a connection.
    /// Also create the future that can be returned to the client.
    ///
    /// On a failure return the created future and also the sender that was not sent to the inner
    /// pool to reuse it for a subsequent attempt
    pub(crate) fn check_out<'a, M: Into<CheckoutConstraint> + 'a>(
        &'a self,
        constraint: M,
    ) -> impl Future<Output = Result<Managed<T>, FailedCheckout>> + 'a {
        let checkout_requested_at = Instant::now();
        self.check_out2(checkout_requested_at, constraint)
    }

    // Takes a checkout future and a `CheckoutPackage`.
    // If the package can be sent to the inner pool the future will be returned immediately.
    // Otherwise the future will be returned alongside the package to use the parts for
    // a retry.
    pub(crate) async fn check_out2<M: Into<CheckoutConstraint>>(
        &self,
        checkout_requested_at: Instant,
        constraint: M,
    ) -> Result<Managed<T>, FailedCheckout> {
        let constraint = constraint.into();

        if constraint.is_deadline_elapsed() {
            return Err(FailedCheckout::new(checkout_requested_at, CheckoutErrorKind::CheckoutTimeout));
        }

        let (deadline, reservation_allowed) = constraint.deadline_and_reservation_allowed();

        let (tx, rx) = oneshot::channel();

        // We need a future to return to the client and a `CheckoutPackage`
        // to send to the inner pool to complete the checkout
        let payload = CheckoutPayload {
            checkout_requested_at,
            sender: tx,
            reservation_allowed,
        };
        let request = CheckoutRequest {
            created_at: Instant::now(),
            payload,
        };
        let mut checkout_sink = self.checkout_sink.clone();

        if let Err(err) = checkout_sink.try_send(request) {
            let error_kind = match err {
                TrySendError::Full(_) => CheckoutErrorKind::CheckoutLimitReached,
                TrySendError::Closed(_) => CheckoutErrorKind::NoPool,
            };

            return Err(FailedCheckout::new(checkout_requested_at, error_kind));
        }

        let managed = match deadline {
            None => rx.await,
            Some(deadline) => time::timeout_at(deadline.into(), rx).await
                .map_err(|_timeout| FailedCheckout::new(checkout_requested_at, CheckoutErrorKind::CheckoutTimeout))?
        }
        .map_err(|_receive_error| FailedCheckout::new(checkout_requested_at, CheckoutErrorKind::NoPool))?
        .map_err(|checkout_error| FailedCheckout::new(checkout_requested_at, checkout_error.kind()))?;

        Ok(managed)
    }

    pub fn connected_to(&self) -> &str {
        self.extended_connection_factory.connecting_to()
    }

    pub fn state(&self) -> PoolState {
        self.extended_connection_factory.instrumentation.state()
    }

    pub fn ping(&self, timeout: Instant) -> BoxFuture<Ping> {
        self.extended_connection_factory.ping(timeout)
    }

    #[cfg(test)]
    pub fn custom_instrumentation<C, I>(
        config: Config,
        connection_factory: C,
        executor: ExecutorFlavour,
        instrumentation: I,
    ) -> Self
    where
        I: crate::instrumentation::Instrumentation + Send + Sync + 'static,
        C: ConnectionFactory<Connection = T> + Send + Sync + 'static,
    {
        Self::new(
            config,
            connection_factory,
            executor,
            PoolInstrumentation::new(
                InstrumentationFlavour::Custom(Arc::new(instrumentation)),
                PoolId::new(0),
            ),
        )
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
            PoolInstrumentation::new(InstrumentationFlavour::NoInstrumentation, PoolId::new(0)),
        )
    }
}

impl<T: Poolable> Drop for PoolInternal<T> {
    fn drop(&mut self) {
        trace!("Dropping PoolInternal {}", self.connected_to());

        let sender = self.extended_connection_factory.send_back_cloned();
        // Stop the internal stream manually and forcefully. Otherwise it
        // will stay alive as long as a client does not return a connection.
        let _ = sender.send(PoolMessageEnvelope::Stop);

        self.extended_connection_factory
            .instrumentation
            .pool_removed();
    }
}

impl<T: Poolable> fmt::Debug for PoolInternal<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PoolInternal")
    }
}

/// An attempt to send a checkout to the inner pool failed.
pub(crate) struct FailedCheckout {
    pub error_kind: CheckoutErrorKind,
    pub checkout_requested_at: Instant,
}

impl FailedCheckout {
    pub fn new(checkout_requested_at: Instant, error_kind: CheckoutErrorKind) -> Self {
        Self {
            checkout_requested_at,
            error_kind,
        }
    }
}

fn start_inner_pool_consumer<T: Poolable>(
    mut pool: InnerPool<T>,
    checkout_receiver: mpsc::Receiver<CheckoutRequest<T>>,
    internal_receiver: mpsc::UnboundedReceiver<PoolMessageEnvelope<T>>,
    executor: &ExecutorFlavour,
) {
    let _ = executor.spawn(async move {
        let checkout_stream = checkout_receiver
            .map(|rq| {
                PoolMessageEnvelope::PoolMessage(PoolMessage::CheckOut {
                    created_at: rq.created_at,
                    payload: rq.payload,
                })
            });

        let mut merged_stream = stream::select(internal_receiver, checkout_stream);

        while let Some(message) = merged_stream.next().await {
            match message {
                PoolMessageEnvelope::PoolMessage(message) => pool.process(message),
                PoolMessageEnvelope::Stop => {
                    trace!("Stopping message stream");
                    break;
                }
            }
        }

        trace!("pool message stream stopped");
    });
}

#[cfg(test)]
mod test;
