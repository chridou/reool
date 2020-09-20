use std::{
    sync::{atomic::AtomicUsize, atomic::Ordering, Arc},
    time::Instant,
};

use crate::{
    pools::pool_internal::PoolInternal,
    pools::{pool_internal::Managed, CheckoutConstraint},
    CheckoutError, Poolable,
};

use super::one_cycle;

#[derive(Clone)]
pub(crate) struct OneCycleImpl {
    pub counter: Arc<AtomicUsize>,
}

impl OneCycleImpl {
    pub async fn apply<T: Poolable>(
        &self,
        constraint: CheckoutConstraint,
        pools: &[PoolInternal<T>],
        first_checkout_attempt_at: Instant,
    ) -> Result<Managed<T>, CheckoutError> {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);
        one_cycle(pools, counter, 0, first_checkout_attempt_at, constraint).await
    }
}

impl Default for OneCycleImpl {
    fn default() -> Self {
        OneCycleImpl {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}
