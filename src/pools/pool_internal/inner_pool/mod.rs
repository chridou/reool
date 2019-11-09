use std::error::Error as StdError;
use std::fmt;
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace, warn};
use tokio_timer::Timeout;

use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::pooled_connection::ConnectionFlavour;
use crate::{config::PoolCheckoutMode, Ping, PingState, Poolable};

use super::{CheckoutManaged, Config, Managed, NewConnMessage};

use self::core::{Core, CoreGuard, SyncCore};
use super::instrumentation::PoolInstrumentation;
mod core;

pub(crate) struct InnerPool<T: Poolable> {
    sync_core: SyncCore<T>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    reservation_limit: Option<usize>,
    instrumentation: PoolInstrumentation,
    connected_to: Vec<String>,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(
        connected_to: Vec<String>,
        config: &Config,
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
            reservation_limit: config.reservation_limit,
            instrumentation,
            connected_to,
        }
    }

    pub(super) fn check_in(
        &self,
        parcel: CheckInParcel<T>,
    ) -> impl Future<Item = (), Error = ()> + Send {
        match parcel {
            CheckInParcel::Alive(managed) => Box::new(self.check_in_alive(managed)),
            CheckInParcel::Dropped(in_flight_time, life_time) => {
                if let Some(in_flight_time) = in_flight_time {
                    self.instrumentation
                        .connection_dropped(Some(in_flight_time), life_time);
                    self.instrumentation.in_flight_dec();
                } else {
                    warn!("no in flight time for dropped connection - this is a bug");
                }

                Box::new(future::ok(())) as Box<dyn Future<Item = _, Error = _> + Send>
            }
        }
    }

    fn check_in_alive(&self, mut managed: Managed<T>) -> impl Future<Item = (), Error = ()> + Send {
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

        self.sync_core.lock().map(|mut core| {
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
        })
    }

    pub(super) fn check_out(&self, mode: PoolCheckoutMode) -> CheckoutManaged<T> {
        let mode = mode.adjust();
        let reservation_limit = self.reservation_limit;
        CheckoutManaged::new(
            self.sync_core
                .lock()
                .map_err(|()| CheckoutError::from(CheckoutErrorKind::TaskExecution))
                .and_then(move |mut core| {
                    if let Some((mut managed, idle_since)) = { core.idle.get() } {
                        trace!("check out - checking out idle connection");
                        managed.checked_out_at = Some(Instant::now());

                        core.instrumentation.checked_out_connection(idle_since);
                        core.instrumentation.idle_dec();
                        core.instrumentation.in_flight_inc();

                        CheckoutManaged::new(future::ok(managed))
                    } else {
                        trace!("check out - no idle connection");
                        match mode {
                            PoolCheckoutMode::Immediately => {
                                // This failed. There was no connection to delivery immediately...
                                return CheckoutManaged::new(future::err(CheckoutError::new(
                                    CheckoutErrorKind::NoConnection,
                                )));
                            }
                            PoolCheckoutMode::Wait => {
                                Self::create_reservation(core, None, reservation_limit)
                            }
                            PoolCheckoutMode::WaitAtMost(d) => {
                                Self::create_reservation(core, Some(d), reservation_limit)
                            }
                        }
                    }
                }),
        )
    }

    fn create_reservation(
        mut core: CoreGuard<T>,
        timeout: Option<Duration>,
        reservation_limit: Option<usize>,
    ) -> CheckoutManaged<T> {
        if let Some(reservation_limit) = reservation_limit {
            if core.reservations.len() == reservation_limit {
                trace!(
                    "check out - reservation limit reached \
                     - returning error"
                );

                if reservation_limit == 0 {
                    return CheckoutManaged::new(future::err(CheckoutError::new(
                        CheckoutErrorKind::NoConnection,
                    )));
                } else {
                    core.instrumentation.reservation_limit_reached();
                    return CheckoutManaged::new(future::err(CheckoutError::new(
                        CheckoutErrorKind::QueueLimitReached,
                    )));
                }
            }
        }

        let (tx, rx) = oneshot::channel();
        let waiting = Reservation::checkout(tx);
        core.reservations.push_back(waiting);

        let fut = rx
            .map(From::from)
            .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::NoConnection, err));
        let fut = if let Some(timeout) = timeout {
            let timeout_fut = Timeout::new(fut, timeout)
                .map_err(|err| CheckoutError::with_cause(CheckoutErrorKind::CheckoutTimeout, err));
            CheckoutManaged::new(timeout_fut)
        } else {
            CheckoutManaged::new(fut)
        };

        core.instrumentation.reservation_added();

        drop(core);

        fut
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

        let single_connected_to = if self.connected_to.len() == 1 {
            Some(self.connected_to[0].to_string())
        } else {
            None
        };

        let f = crate::Checkout(self.check_out(PoolCheckoutMode::Wait))
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
                            single_connected_to,
                            PingState::Failed(Box::new(PingError(format!(
                                "ping time out of {:?} reached",
                                timeout
                            )))),
                        )
                    } else {
                        (
                            single_connected_to,
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
