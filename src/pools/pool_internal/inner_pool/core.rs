use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::ops;
use std::time::{Duration, Instant};

use futures::sync::oneshot;

use super::Managed;
use crate::activation_order::ActivationOrder;
use crate::Poolable;

use super::super::instrumentation::PoolInstrumentation;

/// Used to ensure there is no race between checkouts and puts
pub(super) struct Core<T: Poolable> {
    pub idle: IdleConnections<Managed<T>>,
    pub reservations: VecDeque<Reservation<T>>,
}

impl<T: Poolable> Core<T> {
    pub fn new(desired_pool_size: usize, activation_order: ActivationOrder) -> Self {
        Self {
            idle: IdleConnections::new(desired_pool_size, activation_order),
            reservations: VecDeque::default(),
        }
    }
}

// ===== SYNC CORE ======

pub(super) struct SyncCore<T: Poolable> {
    mutex: Mutex<Core<T>>,
    instrumentation: PoolInstrumentation,
}

impl<T: Poolable> SyncCore<T> {
    pub fn new(core: Core<T>, instrumentation: PoolInstrumentation) -> Self {
        Self {
            mutex: Mutex::new(core),
            instrumentation,
        }
    }

    pub fn lock(&self) -> CoreGuard<T> {
        self.instrumentation.reached_lock();
        let reached_lock_at = Instant::now();
        let guard = self.mutex.lock();
        let lock_released_at = Instant::now();

        self.instrumentation.passed_lock(reached_lock_at.elapsed());

        CoreGuard {
            inner: guard,
            lock_released_at,
            instrumentation: self.instrumentation.clone(),
        }
    }
}

pub(super) struct CoreGuard<'a, T: Poolable> {
    inner: MutexGuard<'a, Core<T>>,
    lock_released_at: Instant,
    instrumentation: PoolInstrumentation,
}

impl<'a, T: Poolable> ops::Deref for CoreGuard<'a, T> {
    type Target = Core<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'a, T: Poolable> ops::DerefMut for CoreGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<'a, T: Poolable> Drop for CoreGuard<'a, T> {
    fn drop(&mut self) {
        self.instrumentation
            .lock_released(self.lock_released_at.elapsed());
    }
}

// ===== IDLE SLOT =====

pub struct IdleSlot<T>(T, Instant);

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
            IdleConnections::FiFo(idle) => idle.push_back(IdleSlot(conn, Instant::now())),
            IdleConnections::LiFo(idle) => idle.push(IdleSlot(conn, Instant::now())),
        }
    }

    #[inline]
    pub fn get(&mut self) -> Option<(T, Duration)> {
        match self {
            IdleConnections::FiFo(idle) => idle.pop_front(),
            IdleConnections::LiFo(idle) => idle.pop(),
        }
        .map(|IdleSlot(conn, idle_since)| (conn, idle_since.elapsed()))
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            IdleConnections::FiFo(idle) => idle.len(),
            IdleConnections::LiFo(idle) => idle.len(),
        }
    }
}

// ===== RESERVATION =====

pub(super) enum Reservation<T: Poolable> {
    Checkout(oneshot::Sender<Managed<T>>, Instant),
    ReducePoolSize(Instant),
}

pub(super) enum Fulfillment<T: Poolable> {
    NotFulfilled(Managed<T>, Duration),
    Reservation(Duration),
    Killed(Duration),
}

impl<T: Poolable> Reservation<T> {
    pub fn checkout(sender: oneshot::Sender<Managed<T>>) -> Self {
        Reservation::Checkout(sender, Instant::now())
    }

    pub fn reduce_pool_size() -> Self {
        Reservation::ReducePoolSize(Instant::now())
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
            Reservation::ReducePoolSize(waiting_since) => {
                managed.checked_out_at = None;
                managed.marked_for_kill = true;
                Fulfillment::Killed(waiting_since.elapsed())
            }
        }
    }
}
