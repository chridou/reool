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
pub(crate) struct TwoAttemptsImpl {
    pub counter: Arc<AtomicUsize>,
}

impl TwoAttemptsImpl {
    pub async fn apply<T: Poolable>(
        &self,
        constraint: CheckoutConstraint,
        pools: &[PoolInternal<T>],
        first_checkout_attempt_at: Instant,
    ) -> Result<Managed<T>, CheckoutError> {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);

        if let Ok(conn) = get_pool(counter, 0, pools)
            .check_out_with_timestamp(CheckoutConstraint::Immediately, first_checkout_attempt_at)
            .await
        {
            return Ok(conn);
        }

        get_pool(counter, 1, pools)
            .check_out_with_timestamp(constraint, first_checkout_attempt_at)
            .await
    }
}

impl Default for TwoAttemptsImpl {
    fn default() -> Self {
        TwoAttemptsImpl {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}
