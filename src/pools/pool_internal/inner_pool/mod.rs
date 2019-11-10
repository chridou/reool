use std::error::Error as StdError;
use std::fmt;
use std::time::{Duration, Instant};

use futures::{
    future::{Future, FutureExt, TryFutureExt},
    channel::{mpsc, oneshot},
};
use log::{debug, error, trace, warn};
use tokio::future::FutureExt as _;

use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::pooled_connection::ConnectionFlavour;
use crate::{Ping, PingState, Poolable, RedisConnection};

use super::{Config, Managed, NewConnMessage};

use self::core::{Core, CoreGuard, SyncCore};
use super::instrumentation::PoolInstrumentation;
mod core;

pub(crate) struct InnerPool<T: Poolable> {
    sync_core: SyncCore<T>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
    instrumentation: PoolInstrumentation,
    connected_to: Vec<String>,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(
        connected_to: Vec<String>,
        config: Config,
        request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
        instrumentation: PoolInstrumentation,
    ) -> Self {
        let sync_core = SyncCore::new(Core::new(
            config.desired_pool_size,
            config.activation_order,
            instrumentation.clone(),
        ));

        instrumentation.pool_added();

        Self {
            sync_core,
            request_new_conn,
            config,
            instrumentation,
            connected_to,
        }
    }

    pub(super) async fn check_in(
        &self,
        parcel: CheckInParcel<T>,
    ) {
        match parcel {
            CheckInParcel::Alive(managed) => self.check_in_alive(managed).await,
            CheckInParcel::Dropped(in_flight_time, life_time) => {
                if let Some(in_flight_time) = in_flight_time {
                    self.instrumentation
                        .connection_dropped(Some(in_flight_time), life_time);
                    self.instrumentation.in_flight_dec();
                } else {
                    warn!("no in flight time for dropped connection - this is a bug");
                }
            }
        }
    }

    async fn check_in_alive(&self, mut managed: Managed<T>) {
        let checked_out_at = managed.checked_out_at.take();

        if let Some(checked_out_at) = checked_out_at {
            trace!("check in - returning connection",);
            self.instrumentation
                .checked_in_returned_connection(checked_out_at.elapsed());
            self.instrumentation.in_flight_dec();
        } else {
            trace!("check in - new connection");
            self.instrumentation.checked_in_new_connection();
        }

        let mut core = self.sync_core.lock().await;

        if core.reservations.is_empty() {
            core.idle.put(managed);
            trace!(
                "check in - no reservations - added to idle - ide: {}",
                core.idle.len()
            );
            core.instrumentation.idle_inc();
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.reservations.pop_front() {
                match one_waiting.try_fulfill(to_fulfill) {
                    Fulfillment::Reservation(waited_for) => {
                        trace!("fulfill reservation - fulfilled - in-flight");

                        core.instrumentation
                            .checked_out_connection(Duration::from_secs(0));
                        core.instrumentation.reservation_fulfilled(waited_for);
                        core.instrumentation.in_flight_inc();

                        return;
                    }
                    Fulfillment::NotFulfilled(not_fulfilled, waited_for) => {
                        trace!("fulfill reservation - not fulfilled");
                        core.instrumentation.reservation_not_fulfilled(waited_for);

                        to_fulfill = not_fulfilled;
                    }
                }
            }

            core.idle.put(to_fulfill);
            let num_idle = core.idle.len();
            core.instrumentation.idle_inc();
            trace!("check in - none fulfilled - added to idle {}", num_idle);
        }
    }

    pub(super) async fn check_out(&self, timeout: Option<Duration>) -> Result<Managed<T>, CheckoutError> {
        let reservation_limit = self.config.reservation_limit;

        let mut core = self.sync_core.lock().await;

        if let Some((mut managed, idle_since)) = core.idle.get() {
            trace!("check out - checking out idle connection");
            managed.checked_out_at = Some(Instant::now());

            core.instrumentation.checked_out_connection(idle_since);
            core.instrumentation.idle_dec();
            core.instrumentation.in_flight_inc();

            Ok(managed)
        } else {
            if let Some(reservation_limit) = reservation_limit {
                if core.reservations.len() == reservation_limit {
                    trace!(
                        "check out - reservation limit reached \
                            - returning error"
                    );

                    let err = if reservation_limit == 0 {
                        CheckoutError::new(CheckoutErrorKind::NoConnection)
                    } else {
                        core.instrumentation.reservation_limit_reached();
                        CheckoutError::new(CheckoutErrorKind::QueueLimitReached)
                    };

                    return Err(err);
                }
            }

            trace!(
                "check out - no idle connection - \
                    enqueue reservation"
            );

            Self::create_reservation(timeout, core).await
        }
    }

    fn create_reservation(timeout: Option<Duration>, mut core: CoreGuard<'_, T>) -> impl Future<Output = Result<Managed<T>, CheckoutError>> {
        let (tx, rx) = oneshot::channel::<Managed<T>>();
        let waiting = Reservation::checkout(tx);

        core.reservations.push_back(waiting);
        core.instrumentation.reservation_added();

        drop(core);

        async move {
            let managed = rx
                .map_ok(Managed::<T>::from)
                .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::NoConnection, err));

            if let Some(timeout) = timeout {
                managed.timeout(timeout)
                    .await
                    .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::CheckoutTimeout, err))?
            } else {
                managed.await
            }
        }
    }

    pub(super) fn request_new_conn(&self) {
        if self
            .request_new_conn
            .unbounded_send(NewConnMessage::RequestNewConn)
            .is_err()
        {
            error!("could not request a new connection")
        }
    }

    // ==== Instrumentation ====

    #[inline]
    pub(super) fn notify_connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
    ) {
        self.instrumentation
            .connection_created(connected_after, total_time)
    }

    #[inline]
    pub(super) fn notify_connection_factory_failed(&self) {
        self.instrumentation.connection_factory_failed();
    }

    // === OTHER ===

    pub fn connected_to(&self) -> &[String] {
        &self.connected_to
    }
}

impl InnerPool<ConnectionFlavour> {
    pub async fn ping(&self, timeout: Duration) -> Ping {
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

        let single_connected_to = if self.connected_to.len() == 1 {
            Some(self.connected_to[0].to_string())
        } else {
            None
        };

        let result =
            async {
                let mut conn = self.check_out(Some(timeout)).await
                    .map(RedisConnection::from_ok_managed)
                    .map_err(|err| (Box::new(err) as Box<dyn StdError + Send>, None))?;

                let connected_to = conn.connected_to().to_owned();

                match conn.ping().await {
                    Ok(_) => Ok(connected_to),
                    Err(err) => Err((
                        Box::new(err) as Box<dyn StdError + Send>,
                        Some(connected_to),
                    )),
                }
            }
            .timeout(timeout)
            .await;

        let (uri, state) = match result {
            Ok(Ok(uri)) => (Some(uri), PingState::Ok),
            Ok(Err((err, uri))) => (uri, PingState::Failed(err)),
            Err(err) => (
                single_connected_to,
                PingState::Failed(Box::new(PingError(format!(
                    "ping time out of {:?} reached",
                    timeout
                )))),
            ),
        };

        Ping {
            uri,
            latency: started_at.elapsed(),
            state,
        }
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        let _ = self
            .request_new_conn
            .unbounded_send(NewConnMessage::Shutdown);

        self.instrumentation.pool_removed();

        let in_flight = self.instrumentation.in_flight();

        for _ in 0..in_flight {
            self.instrumentation.in_flight_dec();
        }

        debug!("inner pool dropped - all connections will be terminated when returned");
    }
}

// ===== CHECK IN PARCEL =====

pub(super) enum CheckInParcel<T: Poolable> {
    Alive(Managed<T>),
    Dropped(Option<Duration>, Duration),
}

// ===== RESERVATION =====

pub(super) enum Reservation<T: Poolable> {
    Checkout(oneshot::Sender<Managed<T>>, Instant),
}

pub(super) enum Fulfillment<T: Poolable> {
    NotFulfilled(Managed<T>, Duration),
    Reservation(Duration),
}

impl<T: Poolable> Reservation<T> {
    pub fn checkout(sender: oneshot::Sender<Managed<T>>) -> Self {
        Reservation::Checkout(sender, Instant::now())
    }
}

impl<T: Poolable> Reservation<T> {
    pub fn try_fulfill(self, mut managed: Managed<T>) -> Fulfillment<T> {
        match self {
            Reservation::Checkout(sender, waiting_since) => {
                managed.checked_out_at = Some(Instant::now());
                if let Err(mut managed) = sender.send(managed) {
                    managed.checked_out_at = None;
                    Fulfillment::NotFulfilled(managed, waiting_since.elapsed())
                } else {
                    Fulfillment::Reservation(waiting_since.elapsed())
                }
            }
        }
    }
}
