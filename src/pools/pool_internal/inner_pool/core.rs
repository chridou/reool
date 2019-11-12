use std::collections::VecDeque;
use std::ops;
use std::time::{Duration, Instant};

use futures::{future::Future, Async, Poll};
use tokio::sync::lock::{Lock, LockGuard};

use crate::activation_order::ActivationOrder;
use crate::Poolable;

use super::super::instrumentation::PoolInstrumentation;

use super::{Managed, Reservation};

/// Used to ensure there is no race between checkouts and puts
pub(super) struct Core<T: Poolable> {
    idle: IdleConnections<Managed<T>>,
    pub reservations: VecDeque<Reservation<T>>,
    pub instrumentation: PoolInstrumentation,
}

impl<T: Poolable> Core<T> {
    pub fn new(
        desired_pool_size: usize,
        activation_order: ActivationOrder,
        instrumentation: PoolInstrumentation,
    ) -> Self {
        Self {
            idle: IdleConnections::new(desired_pool_size, activation_order),
            reservations: VecDeque::default(),
            instrumentation,
        }
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

    pub fn idle_count(&self) -> usize {
        self.idle.len()
    }
}

impl<T: Poolable> Drop for Core<T> {
    fn drop(&mut self) {
        let instrumentation = self.instrumentation.clone();
        self.idle.drain().for_each(|c| {
            instrumentation.idle_dec();
            instrumentation.connection_dropped(None, c.0.created_at.elapsed());
        })
    }
}

// ===== SYNC CORE ======

pub(super) struct SyncCore<T: Poolable> {
    lock: Lock<Core<T>>,
    instrumentation: PoolInstrumentation,
}

impl<T: Poolable> SyncCore<T> {
    pub fn new(core: Core<T>) -> Self {
        let instrumentation = core.instrumentation.clone();
        Self {
            lock: Lock::new(core),
            instrumentation,
        }
    }

    pub fn lock(&self) -> Waiting<T> {
        self.instrumentation.reached_lock();
        Waiting {
            lock: self.lock.clone(),
            waiting_since: Instant::now(),
        }
    }
}

pub(super) struct Waiting<T: Poolable> {
    lock: Lock<Core<T>>,
    waiting_since: Instant,
}

impl<T> Future for Waiting<T>
where
    T: Poolable,
{
    type Item = CoreGuard<T>;
    type Error = ();

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.lock.poll_lock() {
            Async::Ready(guard) => {
                guard
                    .instrumentation
                    .passed_lock(self.waiting_since.elapsed());

                Ok(Async::Ready(CoreGuard {
                    guard,
                    lock_entered_at: Instant::now(),
                }))
            }
            Async::NotReady => Ok(Async::NotReady),
        }
    }
}

pub(super) struct CoreGuard<T: Poolable> {
    guard: LockGuard<Core<T>>,
    lock_entered_at: Instant,
}

impl<T: Poolable> ops::Deref for CoreGuard<T> {
    type Target = Core<T>;
    fn deref(&self) -> &Self::Target {
        &self.guard
    }
}

impl<T: Poolable> ops::DerefMut for CoreGuard<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.guard
    }
}

impl<T: Poolable> Drop for CoreGuard<T> {
    fn drop(&mut self) {
        self.guard
            .instrumentation
            .lock_released(self.lock_entered_at.elapsed());
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

    pub fn drain<'a>(&'a mut self) -> impl Iterator<Item = IdleSlot<T>> + 'a {
        match self {
            IdleConnections::FiFo(ref mut idle) => Box::new(idle.drain(..)),
            IdleConnections::LiFo(ref mut idle) => {
                Box::new(idle.drain(..)) as Box<dyn Iterator<Item = IdleSlot<T>>>
            }
        }
    }
}
