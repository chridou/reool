use std::collections::VecDeque;
use std::ops;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, MutexGuard};

use crate::activation_order::ActivationOrder;
use crate::Poolable;

use super::super::instrumentation::PoolInstrumentation;

use super::{Managed, Reservation};

/// Used to ensure there is no race between checkouts and puts
pub(super) struct Core<T: Poolable> {
    pub idle: IdleConnections<Managed<T>>,
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
}

impl<T: Poolable> Drop for Core<T> {
    fn drop(&mut self) {
        let instrumentation = self.instrumentation.clone();
        self.idle.drain().for_each(|c| {
            instrumentation.connection_dropped(None, c.0.created_at.elapsed());
        })
    }
}

// ===== SYNC CORE ======

pub(super) struct SyncCore<T: Poolable> {
    core: Mutex<Core<T>>,
    instrumentation: PoolInstrumentation,
}

impl<T: Poolable> SyncCore<T> {
    pub fn new(core: Core<T>) -> Self {
        let instrumentation = core.instrumentation.clone();
        Self {
            core: Mutex::new(core),
            instrumentation,
        }
    }

    pub async fn lock(&self) -> CoreGuard<'_, T> {
        self.instrumentation.reached_lock();

        let waiting_since = Instant::now();
        let core = self.core.lock().await;
        let lock_entered_at = Instant::now();
        let wait_duration = lock_entered_at.duration_since(waiting_since); 

        self.instrumentation.passed_lock(wait_duration);

        CoreGuard {
            core,
            lock_entered_at,
        }
    }
}

pub(super) struct CoreGuard<'a, T: Poolable> {
    core: MutexGuard<'a, Core<T>>,
    lock_entered_at: Instant,
}

impl<'a, T: Poolable> ops::Deref for CoreGuard<'a, T> {
    type Target = Core<T>;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl<'a, T: Poolable> ops::DerefMut for CoreGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.core
    }
}

impl<'a, T: Poolable> Drop for CoreGuard<'a, T> {
    fn drop(&mut self) {
        self.core
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
