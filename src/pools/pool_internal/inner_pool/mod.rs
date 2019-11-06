use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    sync::{mpsc, oneshot},
};
use log::{debug, error, trace};
use tokio_timer::Timeout;

use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::instrumentation::Instrumentation;
use crate::{stats::PoolStats, Poolable};

use super::{CheckoutManaged, Config, Managed, NewConnMessage};

use self::core::{Core, CoreGuard, Fulfillment, Reservation, SyncCore};

mod core;

pub(crate) struct InnerPool<T: Poolable> {
    sync_core: SyncCore<T>,
    request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
    config: Config,
    instrumentation: Option<Arc<dyn Instrumentation + Send + Sync>>,
    connected_to: Arc<Vec<String>>,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new<I>(
        connected_to: Vec<String>,
        config: Config,
        request_new_conn: mpsc::UnboundedSender<NewConnMessage>,
        instrumentation: Option<I>,
    ) -> Self
    where
        I: Instrumentation + Send + Sync + 'static,
    {
        let instrumentation =
            instrumentation.map(|i| Arc::new(i) as Arc<dyn Instrumentation + Send + Sync>);
        let sync_core = SyncCore::new(
            Core::new(
                config.desired_pool_size,
                config.stats_interval,
                config.activation_order,
                connected_to.len(),
            ),
            instrumentation.clone(),
        );

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
                let mut core = self.sync_core.lock();
                core.total_connections_tracker.dec();
                core.in_flight_tracker.dec();
                unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
                if let Some((instrumentation, flight_time)) = self
                    .instrumentation
                    .as_ref()
                    .and_then(|i| in_flight_time.map(|ft| (i, ft)))
                {
                    instrumentation.connection_dropped(flight_time, life_time);
                }
            }
            CheckInParcel::Killed(life_time) => {
                let mut core = self.sync_core.lock();
                core.total_connections_tracker.dec();
                debug!(
                    "connection killed - pool size {}",
                    core.total_connections_tracker.current()
                );
                unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
                if let Some(instrumentation) = self.instrumentation.as_ref() {
                    instrumentation.connection_killed(life_time);
                }
            }
        }
    }

    fn check_in_alive(&self, mut managed: Managed<T>) {
        let checked_out_at = managed.checked_out_at.take();

        if let Some(instrumentation) = self.instrumentation.as_ref() {
            if let Some(checked_out_at) = checked_out_at {
                instrumentation.checked_in_returned_connection(checked_out_at.elapsed());
            } else {
                instrumentation.checked_in_new_connection();
            }
        }

        let mut core = self.sync_core.lock();

        if checked_out_at.is_none() {
            core.total_connections_tracker.inc();
            trace!(
                "check in - new connection - pool size {}",
                core.total_connections_tracker.current()
            );
        } else {
            core.in_flight_tracker.dec();
            trace!(
                "check in - returning connection - in flight {}",
                core.in_flight_tracker.current()
            );
        }

        if core.reservations.is_empty() {
            core.idle.put(managed);
            core.idle_tracker.inc();
            trace!(
                "check in - no reservations - added to idle {}",
                core.idle_tracker.current()
            );
        } else {
            // Do not let this one get dropped!
            let mut to_fulfill = managed;
            while let Some(one_waiting) = core.reservations.pop_front() {
                core.reservations_tracker.dec();
                match one_waiting.try_fulfill(to_fulfill) {
                    Fulfillment::Reservation(waited_for) => {
                        core.in_flight_tracker.inc();
                        trace!(
                            "fulfill reservation - fulfilled - in-flight {}",
                            core.in_flight_tracker.current()
                        );

                        unlock_then_publish_stats(
                            core,
                            self.instrumentation.as_ref().map(|i| &**i),
                        );

                        if let Some(instrumentation) = self.instrumentation.as_ref() {
                            instrumentation.checked_out_connection(Duration::from_secs(0));
                            instrumentation.reservation_fulfilled(waited_for);
                        }

                        return;
                    }
                    Fulfillment::Killed => {
                        core.total_connections_tracker.dec();
                        trace!(
                            "reservation - killed - pool size {}",
                            core.total_connections_tracker.current()
                        );

                        unlock_then_publish_stats(
                            core,
                            self.instrumentation.as_ref().map(|i| &**i),
                        );
                        return;
                    }
                    Fulfillment::NotFulfilled(not_fulfilled, waited_for) => {
                        trace!("fulfill reservation - not fulfilled");
                        if let Some(instrumentation) = self.instrumentation.as_ref() {
                            instrumentation.reservation_not_fulfilled(waited_for)
                        }

                        to_fulfill = not_fulfilled;
                    }
                }
            }

            core.idle.put(to_fulfill);
            let idle_count = core.idle.len();
            let reservations_count = core.reservations.len();
            core.idle_tracker.set(idle_count);
            core.reservations_tracker.set(reservations_count);
            trace!(
                "check in - none fulfilled - added to idle {}",
                core.idle_tracker.current()
            );
        }

        unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
    }

    pub(super) fn check_out(&self, timeout: Option<Duration>) -> CheckoutManaged<T> {
        let mut core = self.sync_core.lock();

        if let Some((mut managed, idle_since)) = { core.idle.get() } {
            trace!("check out - checking out idle connection");
            managed.checked_out_at = Some(Instant::now());
            core.idle_tracker.dec();
            core.in_flight_tracker.inc();

            unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));

            if let Some(instrumentation) = self.instrumentation.as_ref() {
                instrumentation.checked_out_connection(idle_since);
            }

            CheckoutManaged::new(future::ok(managed))
        } else {
            if let Some(reservation_limit) = self.config.reservation_limit {
                if core.reservations.len() > reservation_limit {
                    trace!(
                        "check out - reservation limit reached \
                         - returning error"
                    );

                    unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));

                    if let Some(instrumentation) = self.instrumentation.as_ref() {
                        instrumentation.reservation_limit_reached()
                    }

                    return CheckoutManaged::new(future::err(CheckoutError::new(
                        CheckoutErrorKind::QueueLimitReached,
                    )));
                }
            }
            trace!(
                "check out - no idle connection - \
                 enqueue reservation"
            );
            Self::create_reservation(timeout, core, self.instrumentation.as_ref().map(|i| &**i))
        }
    }

    fn create_reservation(
        timeout: Option<Duration>,
        mut core: CoreGuard<T>,
        instrumentation: Option<&(dyn Instrumentation + Send + Sync)>,
    ) -> CheckoutManaged<T> {
        let (tx, rx) = oneshot::channel();
        let waiting = Reservation::checkout(tx);
        core.reservations.push_back(waiting);
        core.reservations_tracker.inc();

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

        unlock_then_publish_stats(core, instrumentation);

        if let Some(instrumentation) = instrumentation {
            instrumentation.reservation_added()
        }

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
            core.idle_tracker.dec();
            drop(core);
            managed.marked_for_kill = true;
        } else {
            trace!("no idle connection to kill - enqueue for kill");
            core.reservations.push_back(Reservation::reduce_pool_size());
            core.reservations_tracker.inc();
            unlock_then_publish_stats(core, self.instrumentation.as_ref().map(|i| &**i));
            if let Some(instrumentation) = self.instrumentation.as_ref() {
                instrumentation.reservation_added()
            }
        }
    }

    // ==== Instrumentation ====

    #[inline]
    pub(super) fn notify_connection_created(
        &self,
        connected_after: Duration,
        total_time: Duration,
    ) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.connection_created(connected_after, total_time)
        }
    }

    #[inline]
    pub(super) fn notify_connection_factory_failed(&self) {
        if let Some(instrumentation) = self.instrumentation.as_ref() {
            instrumentation.connection_factory_failed()
        }
    }

    pub fn stats(&self) -> PoolStats {
        self.sync_core.lock().stats()
    }

    pub fn trigger_stats(&self) {
        unlock_then_publish_stats(
            self.sync_core.lock(),
            self.instrumentation.as_ref().map(|i| &**i),
        );
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

        if let Some(instr) = self.instrumentation.as_ref() {
            instr.stats(PoolStats::default())
        }

        debug!("inner pool dropped - all connections will be terminated when returned");
    }
}

// ===== CHECK IN PARCEL =====

pub(super) enum CheckInParcel<T: Poolable> {
    Alive(Managed<T>),
    Dropped(Option<Duration>, Duration),
    Killed(Duration),
}

fn unlock_then_publish_stats<T: Poolable>(
    mut core: CoreGuard<T>,
    instrumentation: Option<&(dyn Instrumentation + Send + Sync)>,
) {
    let snapshot = core.try_flush();
    drop(core);
    if let (Some(instr), Some(snapshot)) = (instrumentation, snapshot) {
        instr.stats(snapshot)
    }
}
