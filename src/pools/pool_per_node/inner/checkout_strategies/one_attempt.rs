use std::{
    sync::{atomic::AtomicUsize, atomic::Ordering, Arc},
    time::Instant,
};

use crate::{
    pools::pool_internal::PoolInternal,
    pools::{pool_internal::Managed, CheckoutConstraint},
    CheckoutError, Poolable,
};

use super::get_pool;

#[derive(Clone)]
pub(crate) struct OneAttemptImpl {
    pub counter: Arc<AtomicUsize>,
}

impl OneAttemptImpl {
    pub async fn apply<T: Poolable>(
        &self,
        constraint: CheckoutConstraint,
        pools: &[PoolInternal<T>],
        first_checkout_attempt_at: Instant,
    ) -> Result<Managed<T>, CheckoutError> {
        let pool = get_pool(self.counter.fetch_add(1, Ordering::SeqCst), 0, pools);
        pool.check_out_with_timestamp(constraint, first_checkout_attempt_at)
            .await
    }
}

impl Default for OneAttemptImpl {
    fn default() -> Self {
        OneAttemptImpl {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}
