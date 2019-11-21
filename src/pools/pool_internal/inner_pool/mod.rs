use std::collections::VecDeque;
use std::time::{Duration, Instant};

use log::{debug, trace};
use tokio::sync::oneshot;

use crate::config::ActivationOrder;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::Poolable;

use super::instrumentation::PoolInstrumentation;
use super::{Config, Managed};

/// An internal interval sent regularly to clean up reservations
pub(super) const CLEANUP_INTERVAL: Duration = Duration::from_millis(100);

/// A message that can be sent to the inner pool via a stream
pub(crate) enum PoolMessage<T: Poolable> {
    /// Check in a connection
    CheckIn {
        /// Timestamp of message creation
        created_at: Instant,
        /// The connection to check in
        conn: Managed<T>,
    },
    CheckOut {
        /// Timestamp of message creation
        created_at: Instant,
        /// Data containing all data and constraints
        /// needed to do a proper checkout
        payload: CheckoutPayload<T>,
    },
    CleanupReservations(Instant),
    CheckAlive(Instant),
}

impl<T: Poolable> PoolMessage<T> {
    fn is_checkout_and_created_at(&self) -> (bool, Instant) {
        match self {
            PoolMessage::CheckOut { created_at, .. } => (true, *created_at),
            PoolMessage::CheckIn { created_at, .. } => (false, *created_at),
            PoolMessage::CleanupReservations(created_at) => (false, *created_at),
            PoolMessage::CheckAlive(created_at) => (false, *created_at),
        }
    }
}

/// Data containing all data and constraints
/// needed to do a proper checkout
pub(crate) struct CheckoutPayload<T: Poolable> {
    /// A sender which can be used to complete a checkout with a connection
    pub sender: oneshot::Sender<Result<Managed<T>, CheckoutError>>,
    /// The `Instant` when the request for a connection was received
    pub checkout_requested_at: Instant,
    /// `true` if it is allowed to create a reservation.
    pub reservation_allowed: bool,
}

pub(crate) struct InnerPool<T: Poolable> {
    /// Stores the idle connections ready to be checked out
    idle: IdleConnections<Managed<T>>,
    /// Reservations waiting for an incoming connection
    reservations: VecDeque<Reservation<T>>,
    instrumentation: PoolInstrumentation,
    /// Timestamp when the last clean up was done. Used
    /// to only do a cleanup if `CLEANUP_INTERVAL` has already elapsed unless
    /// the reservation queue is already full in which case a cleanup attempt is
    /// always made.
    last_cleanup: Instant,
}

impl<T> InnerPool<T>
where
    T: Poolable,
{
    pub fn new(config: &Config, instrumentation: PoolInstrumentation) -> Self {
        Self {
            idle: IdleConnections::new(config.desired_pool_size, config.activation_order),
            reservations: VecDeque::with_capacity(config.reservation_limit),
            instrumentation,
            last_cleanup: Instant::now(),
        }
    }

    /// Process a PoolMessage
    pub fn process(&mut self, message: PoolMessage<T>) {
        let started_at = Instant::now();

        match message.is_checkout_and_created_at() {
            (true, created_at) => self
                .instrumentation
                .checkout_message_received(created_at.elapsed()),
            (false, created_at) => self
                .instrumentation
                .internal_message_received(created_at.elapsed()),
        }

        match message {
            PoolMessage::CheckIn { conn, .. } => {
                self.check_in(conn);
                self.instrumentation
                    .relevant_message_processed(started_at.elapsed());
            }
            PoolMessage::CheckOut { payload, .. } => {
                self.check_out(payload);
                self.instrumentation
                    .relevant_message_processed(started_at.elapsed());
            }
            PoolMessage::CleanupReservations(_) => {
                self.cleanup_reservations();
                self.instrumentation
                    .relevant_message_processed(started_at.elapsed());
            }
            PoolMessage::CheckAlive(_) => {}
        }
    }

    fn check_in(&mut self, mut managed: Managed<T>) {
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

        if self.reservations.is_empty() {
            self.put_idle(managed);
            trace!(
                "check in - no reservations - added to idle - idle: {}",
                self.idle.len()
            );
        } else {
            // Do not let this one get dropped!
            let mut ready_for_fulfillment = managed;
            while let Some(one_waiting) = self.reservations.pop_front() {
                match one_waiting.try_fulfill(ready_for_fulfillment) {
                    Fulfillment::Fulfilled {
                        reservation_time,
                        time_since_checkout_request,
                    } => {
                        trace!("fulfill reservation - fulfilled");

                        self.instrumentation.checked_out_connection(
                            Duration::from_secs(0),
                            time_since_checkout_request,
                        );
                        self.instrumentation
                            .reservation_fulfilled(reservation_time, time_since_checkout_request);
                        self.instrumentation.in_flight_inc();

                        return;
                    }
                    Fulfillment::NotFulfilled {
                        conn,
                        reservation_time,
                        time_since_checkout_request,
                    } => {
                        trace!("fulfill reservation - not fulfilled");
                        self.instrumentation.reservation_not_fulfilled(
                            reservation_time,
                            time_since_checkout_request,
                        );

                        ready_for_fulfillment = conn;
                    }
                }
            }

            self.put_idle(ready_for_fulfillment);
            trace!(
                "check in - none fulfilled - added to idle {}",
                self.idle.len()
            );
        }
    }

    fn check_out(&mut self, payload: CheckoutPayload<T>) {
        if payload.sender.is_closed() {
            return;
        }

        if let Some((mut managed, idle_since)) = self.get_idle() {
            trace!("check out - checking out idle connection");
            managed.checked_out_at = Some(Instant::now());

            if let Err(Ok(not_send)) = payload.sender.send(Ok(managed)) {
                // The sender is already closed. Put the connection back to
                // the idle queue.
                self.put_idle(not_send);
            } else {
                self.instrumentation
                    .checked_out_connection(idle_since, payload.checkout_requested_at.elapsed());
                self.instrumentation.in_flight_inc();
            }
        } else {
            trace!("check out - no idle connection");

            if payload.reservation_allowed {
                self.create_reservation(payload.sender, payload.checkout_requested_at)
            } else {
                // There was no connection to delivery immediately...
                let _ = payload
                    .sender
                    .send(Err(CheckoutError::new(CheckoutErrorKind::NoConnection)));
            }
        }
    }

    fn create_reservation(
        &mut self,
        sender: oneshot::Sender<Result<Managed<T>, CheckoutError>>,
        checkout_requested_at: Instant,
    ) {
        if self.reservations.capacity() == 0 {
            let _ = sender.send(Err(CheckoutErrorKind::NoConnection.into()));
            return;
        }

        if self.reservations.len() == self.reservations.capacity() {
            self.cleanup_reservations();
            if self.reservations.len() == self.reservations.capacity() {
                self.instrumentation.reservation_limit_reached();
                let _ = sender.send(Err(CheckoutErrorKind::ReservationLimitReached.into()));
                return;
            }
        }

        let reservation = Reservation::new(sender, checkout_requested_at);
        self.reservations.push_back(reservation);

        self.instrumentation.reservation_added();
    }

    pub fn get_idle(&mut self) -> Option<(Managed<T>, Duration)> {
        let idle = self.idle.get();

        if idle.is_some() {
            self.instrumentation.idle_dec();
        }

        idle
    }

    pub fn put_idle(&mut self, conn: Managed<T>) {
        self.instrumentation.idle_inc();
        self.idle.put(conn)
    }

    fn cleanup_reservations(&mut self) {
        if self.reservations.is_empty() {
            // If reservation limit is zero reservations will always be empty
            self.last_cleanup = Instant::now();
            return;
        }

        let cleanup_necessary = self.reservations.len() == self.reservations.capacity()
            || self.last_cleanup.elapsed() > CLEANUP_INTERVAL;

        if cleanup_necessary {
            let instrumentation = self.instrumentation.clone();
            self.reservations.retain(|reservation| {
                if reservation.sender.is_closed() {
                    instrumentation.reservation_not_fulfilled(
                        reservation.created_at.elapsed(),
                        reservation.checkout_requested_at.elapsed(),
                    );
                    return false;
                }

                true
            });
        }

        self.last_cleanup = Instant::now();
    }
}

impl<T: Poolable> Drop for InnerPool<T> {
    fn drop(&mut self) {
        trace!("dropping inner pool");

        let in_flight = self.instrumentation.in_flight();

        for _ in 0..in_flight {
            // These connections will not be able to
            // return to the pool since they are orphans now
            self.instrumentation.in_flight_dec();
        }

        let instrumentation = &self.instrumentation;
        self.idle.drain().for_each(|slot| {
            instrumentation.idle_dec();
            // These connections will trigger the instrumentation
            // since tey are orphans now
            instrumentation.connection_dropped(None, slot.conn.created_at.elapsed());
        });

        debug!(
            "[{}] inner pool dropped - all connections will be terminated when returned",
            self.instrumentation.id
        );
        trace!(
            "inner pool dropped - final state: {:?}",
            self.instrumentation.state()
        );
    }
}

/// ===== RESERVATION =====

/// A reservations waits for a connection to be checked in so that
/// the reservation can be fulfilled
pub(super) struct Reservation<T: Poolable> {
    /// The sender to fulfill the checkout with
    pub sender: oneshot::Sender<Result<Managed<T>, CheckoutError>>,
    /// The instant the reservation was created
    pub created_at: Instant,
    /// The Instant the initial checkout was created at
    pub checkout_requested_at: Instant,
}

impl<T: Poolable> Reservation<T> {
    pub fn new(
        sender: oneshot::Sender<Result<Managed<T>, CheckoutError>>,
        checkout_requested_at: Instant,
    ) -> Self {
        Reservation {
            sender,
            created_at: Instant::now(),
            checkout_requested_at,
        }
    }
}

impl<T: Poolable> Reservation<T> {
    /// Try to send the connection. If successful the reservation was fulfilled.
    /// If failed the reservation was not fulfilled since the receiver
    /// was not there anymore.
    pub fn try_fulfill(self, mut managed: Managed<T>) -> Fulfillment<T> {
        managed.checked_out_at = Some(Instant::now());
        if let Err(Ok(mut managed)) = self.sender.send(Ok(managed)) {
            managed.checked_out_at = None;
            Fulfillment::NotFulfilled {
                conn: managed,
                time_since_checkout_request: self.checkout_requested_at.elapsed(),
                reservation_time: self.created_at.elapsed(),
            }
        } else {
            Fulfillment::Fulfilled {
                time_since_checkout_request: self.checkout_requested_at.elapsed(),
                reservation_time: self.created_at.elapsed(),
            }
        }
    }
}

pub(super) enum Fulfillment<T: Poolable> {
    Fulfilled {
        /// The time it took until the reservation was processed
        reservation_time: Duration,
        /// The time it took from the checkout request being
        /// made until the checkout was actually fulfilled
        time_since_checkout_request: Duration,
    },
    NotFulfilled {
        conn: Managed<T>,
        /// The time it took until the reservation was processed
        reservation_time: Duration,
        /// The time it took from the checkout request being
        /// made until the reservation was processed
        time_since_checkout_request: Duration,
    },
}

// ===== IDLE SLOT =====

pub struct IdleSlot<T> {
    conn: T,
    idle_since: Instant,
}

impl<T> IdleSlot<T> {
    pub fn new(conn: T) -> Self {
        Self {
            conn,
            idle_since: Instant::now(),
        }
    }
}

pub(super) enum IdleConnections<T> {
    FiFo(VecDeque<IdleSlot<T>>),
    LiFo(Vec<IdleSlot<T>>),
}

impl<T> IdleConnections<T> {
    pub fn new(size: usize, activation_order: ActivationOrder) -> Self {
        match activation_order {
            ActivationOrder::FiFo => IdleConnections::FiFo(VecDeque::with_capacity(size)),
            ActivationOrder::LiFo => IdleConnections::LiFo(Vec::with_capacity(size)),
        }
    }

    #[inline]
    pub fn put(&mut self, conn: T) {
        match self {
            IdleConnections::FiFo(idle) => idle.push_back(IdleSlot::new(conn)),
            IdleConnections::LiFo(idle) => idle.push(IdleSlot::new(conn)),
        }
    }

    #[inline]
    pub fn get(&mut self) -> Option<(T, Duration)> {
        match self {
            IdleConnections::FiFo(idle) => idle.pop_front(),
            IdleConnections::LiFo(idle) => idle.pop(),
        }
        .map(|IdleSlot { conn, idle_since }| (conn, idle_since.elapsed()))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            IdleConnections::FiFo(idle) => idle.len(),
            IdleConnections::LiFo(idle) => idle.len(),
        }
    }

    pub fn drain<'a>(&'a mut self) -> impl Iterator<Item = IdleSlot<T>> + 'a {
        match self {
            IdleConnections::FiFo(ref mut idle) => Box::new(idle.drain(..)),
            IdleConnections::LiFo(ref mut idle) => {
                Box::new(idle.drain(..)) as Box<dyn Iterator<Item = IdleSlot<T>>>
            }
        }
    }
}
