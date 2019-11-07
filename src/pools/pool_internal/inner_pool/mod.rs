use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace, warn};
use tokio_timer::Timeout;

use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::Poolable;

use super::{CheckoutManaged, Config, Managed, NewConnMessage};

use self::core::{Core, CoreGuard, Fulfillment, Reservation, SyncCore};
use super::instrumentation::PoolInstrumentation;
mod core;

pub(crate) struct InnerPool<T: Poolable> {
    sync_core: SyncCore<T>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
    instrumentation: PoolInstrumentation,
    connected_to: Arc<Vec<String>>,
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
        let sync_core = SyncCore::new(
            Core::new(config.desired_pool_size, config.activation_order),
            instrumentation.clone(),
        );

        instrumentation.pool_added();

        Self {
            sync_core,
            request_new_conn,
            config,
            instrumentation,
            connected_to: Arc::new(connected_to),
        }
    }

    pub(super) fn check_in(&self, parcel: CheckInParcel<T>) {
        match parcel {
            CheckInParcel::Alive(managed) => self.check_in_alive(managed),
            CheckInParcel::Dropped(in_flight_time, life_time) => {
                if let Some(in_flight_time) = in_flight_time {
                    self.instrumentation
                        .connection_dropped(in_flight_time, life_time);
                } else {
                    warn!("no in flight time for dropped connection - this is a bug");
                }
            }
            CheckInParcel::Killed(life_time) => {
                debug!("connection killed - pool size",);
                self.instrumentation.connection_killed(life_time);
            }
        }
    }

    fn check_in_alive(&self, mut managed: Managed<T>) {
        let checked_out_at = managed.checked_out_at.take();

        if let Some(checked_out_at) = checked_out_at {
            trace!("check in - returning connection",);
            self.instrumentation
                .checked_in_returned_connection(checked_out_at.elapsed());
        } else {
            trace!("check in - new connection");
            self.instrumentation.checked_in_new_connection();
        }

        let mut core = self.sync_core.lock();

        if core.reservations.is_empty() {
            core.idle.put(managed);
            trace!(
                "check in - no reservations - added to idle - ide: {}",
                core.idle.len()
            );
            self.instrumentation.idle_inc();
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.reservations.pop_front() {
                match one_waiting.try_fulfill(to_fulfill) {
                    Fulfillment::Reservation(waited_for) => {
                        trace!("fulfill reservation - fulfilled - in-flight");

                        drop(core);

                        self.instrumentation
                            .checked_out_connection(Duration::from_secs(0));
                        self.instrumentation.reservation_fulfilled(waited_for);

                        return;
                    }
                    Fulfillment::Killed(waited_for) => {
                        drop(core);
                        trace!("reservation - connection killed",);

                        self.instrumentation.reservation_fulfilled(waited_for);
                        return;
                    }
                    Fulfillment::NotFulfilled(not_fulfilled, waited_for) => {
                        trace!("fulfill reservation - not fulfilled");
                        self.instrumentation.reservation_not_fulfilled(waited_for);

                        to_fulfill = not_fulfilled;
                    }
                }
            }

            core.idle.put(to_fulfill);
            let num_idle = core.idle.len();
            drop(core);
            self.instrumentation.idle_inc();
            trace!("check in - none fulfilled - added to idle {}", num_idle);
        }
    }

    pub(super) fn check_out(&self, timeout: Option<Duration>) -> CheckoutManaged<T> {
        let mut core = self.sync_core.lock();

        if let Some((mut managed, idle_since)) = { core.idle.get() } {
            drop(core);
            trace!("check out - checking out idle connection");
            managed.checked_out_at = Some(Instant::now());

            self.instrumentation.checked_out_connection(idle_since);
            self.instrumentation.idle_dec();

            CheckoutManaged::new(future::ok(managed))
        } else {
            if let Some(reservation_limit) = self.config.reservation_limit {
                if core.reservations.len() > reservation_limit {
                    drop(core);
                    trace!(
                        "check out - reservation limit reached \
                         - returning error"
                    );

                    self.instrumentation.reservation_limit_reached();

                    return CheckoutManaged::new(future::err(CheckoutError::new(
                        CheckoutErrorKind::QueueLimitReached,
                    )));
                }
            }
            trace!(
                "check out - no idle connection - \
                 enqueue reservation"
            );
            Self::create_reservation(timeout, core, &self.instrumentation)
        }
    }

    fn create_reservation(
        timeout: Option<Duration>,
        mut core: CoreGuard<T>,
        instrumentation: &PoolInstrumentation,
    ) -> CheckoutManaged<T> {
        let (tx, rx) = oneshot::channel();
        let waiting = Reservation::checkout(tx);
        core.reservations.push_back(waiting);
        drop(core);

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

        instrumentation.reservation_added();

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

    pub(super) fn remove_conn(&self) {
        let mut core = self.sync_core.lock();
        if let Some((mut managed, _)) = { core.idle.get() } {
            self.instrumentation.idle_dec();
            drop(core);
            managed.marked_for_kill = true;
        } else {
            trace!("no idle connection to kill - enqueue for kill");
            core.reservations.push_back(Reservation::reduce_pool_size());
            drop(core);
            self.instrumentation.reservation_added()
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

    pub fn connected_to(&self) -> &[String] {
        &self.connected_to
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        let _ = self
            .request_new_conn
            .unbounded_send(NewConnMessage::Shutdown);

        self.instrumentation.pool_removed();

        debug!("inner pool dropped - all connections will be terminated when returned");
    }
}

// ===== CHECK IN PARCEL =====

pub(super) enum CheckInParcel<T: Poolable> {
    Alive(Managed<T>),
    Dropped(Option<Duration>, Duration),
    Killed(Duration),
}
