use parking_lot::{Mutex, MutexGuard};
use std::collections::VecDeque;
use std::ops;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use super::{Managed, Reservation};
use crate::activation_order::ActivationOrder;
use crate::instrumentation::Instrumentation;
use crate::{
    stats::{MinMax, PoolStats},
    Poolable,
};

/// Used to ensure there is no race between checkouts and puts
pub(super) struct Core<T: Poolable> {
    pub idle: IdleConnections<Managed<T>>,
    pub reservations: VecDeque<Reservation<T>>,
    pub idle_tracker: ValueTracker,
    pub in_flight_tracker: ValueTracker,
    pub reservations_tracker: ValueTracker,
    pub total_connections_tracker: ValueTracker,
    pub stats_interval: Duration,
    pub last_flushed: Instant,
    pub num_nodes_connected_to: usize,
}

impl<T: Poolable> Core<T> {
    pub fn new(
        desired_pool_size: usize,
        stats_interval: Duration,
        activation_order: ActivationOrder,
        num_nodes_connected_to: usize,
    ) -> Self {
        Self {
            idle: IdleConnections::new(desired_pool_size, activation_order),
            reservations: VecDeque::default(),
            idle_tracker: ValueTracker::default(),
            in_flight_tracker: ValueTracker::default(),
            reservations_tracker: ValueTracker::default(),
            total_connections_tracker: ValueTracker::default(),
            stats_interval,
            last_flushed: Instant::now() - stats_interval,
            num_nodes_connected_to,
        }
    }
}

impl<T: Poolable> Core<T> {
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            connections: self.total_connections_tracker.get(),
            in_flight: self.in_flight_tracker.get(),
            reservations: self.reservations_tracker.get(),
            idle: self.idle_tracker.get(),
            node_count: self.num_nodes_connected_to,
            pool_count: 1,
        }
    }

    pub fn try_flush(&mut self) -> Option<PoolStats> {
        let now = Instant::now();
        if self.last_flushed + self.stats_interval >= now {
            None
        } else {
            self.last_flushed = now;
            let current = self.stats();

            self.total_connections_tracker.apply_flush();
            self.in_flight_tracker.apply_flush();
            self.reservations_tracker.apply_flush();
            self.idle_tracker.apply_flush();

            Some(current)
        }
    }
}

// ===== SYNC CORE ======

pub(super) struct SyncCore<T: Poolable> {
    mutex: Mutex<Core<T>>,
    instrumentation: Option<Arc<dyn Instrumentation + Send + Sync>>,
    contention_count: AtomicUsize,
}

impl<T: Poolable> SyncCore<T> {
    pub fn new(
        core: Core<T>,
        instrumentation: Option<Arc<dyn Instrumentation + Send + Sync>>,
    ) -> Self {
        Self {
            mutex: Mutex::new(core),
            instrumentation,
            contention_count: AtomicUsize::new(0),
        }
    }

    pub fn lock(&self) -> CoreGuard<T> {
        let contention_count = self.contention_count.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some(ref instrumentation) = self.instrumentation {
            instrumentation.contention(contention_count);
        }
        let enter_lock_at = Instant::now();
        let guard = self.mutex.lock();

        if let Some(ref instrumentation) = self.instrumentation {
            instrumentation.lock_wait_duration(enter_lock_at);
        }
        self.contention_count.fetch_sub(1, Ordering::SeqCst);

        CoreGuard {
            inner: guard,
            created_instant: Instant::now(),
            instrumentation: self.instrumentation.as_ref().map(Arc::clone),
        }
    }
}

pub(super) struct CoreGuard<'a, T: Poolable> {
    inner: MutexGuard<'a, Core<T>>,
    created_instant: Instant,
    instrumentation: Option<Arc<dyn Instrumentation + Send + Sync>>,
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
            .as_ref()
            .map(|instrumentation| instrumentation.lock_duration(self.created_instant));
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

// ===== VALUE TRACKER ======

pub struct ValueTracker {
    last: usize,
    min_max: Option<MinMax>,
}

impl Default for ValueTracker {
    fn default() -> Self {
        Self {
            last: 0,
            min_max: None,
        }
    }
}

impl ValueTracker {
    #[inline]
    pub fn set(&mut self, v: usize) {
        self.last = v;
        let new_min_max = if let Some(mut min_max) = self.min_max.take() {
            if v < min_max.0 {
                min_max.0 = v;
            }
            if v > min_max.1 {
                min_max.1 = v;
            }
            min_max
        } else {
            MinMax(v, v)
        };
        self.min_max = Some(new_min_max)
    }

    pub fn inc(&mut self) {
        self.set(self.last + 1);
    }

    pub fn dec(&mut self) {
        self.set(self.last - 1);
    }

    #[inline]
    pub fn get(&self) -> MinMax {
        if let Some(min_max) = self.min_max.as_ref() {
            *min_max
        } else {
            MinMax(self.last, self.last)
        }
    }

    #[inline]
    pub fn current(&self) -> usize {
        self.last
    }

    pub fn apply_flush(&mut self) {
        self.min_max = None;
    }
}
