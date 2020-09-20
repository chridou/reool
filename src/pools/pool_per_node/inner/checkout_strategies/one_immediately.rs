use std::{
    sync::{atomic::AtomicUsize, atomic::Ordering, Arc},
    time::Instant,
};

use crate::{
    pools::pool_internal::PoolInternal,
    pools::{pool_internal::Managed, CheckoutConstraint},
    CheckoutError, Poolable,
};

use super::{get_pool, one_cycle};

#[derive(Clone)]
pub struct OneImmediatelyImpl {
    pub counter: Arc<AtomicUsize>,
}

impl OneImmediatelyImpl {
    pub(crate) async fn apply<T: Poolable>(
        &self,
        constraint: CheckoutConstraint,
        pools: &[PoolInternal<T>],
        first_checkout_attempt_at: Instant,
    ) -> Result<Managed<T>, CheckoutError> {
        let counter = self.counter.fetch_add(1, Ordering::SeqCst);

        match get_pool(counter, 0, pools)
            .check_out_with_timestamp(CheckoutConstraint::Immediately, first_checkout_attempt_at)
            .await
        {
            Ok(conn) => return Ok(conn),

            Err(err) => {
                if pools.len() == 1 {
                    return Err(err);
                }
            }
        }

        one_cycle(pools, counter, 1, first_checkout_attempt_at, constraint).await
    }
}

impl Default for OneImmediatelyImpl {
    fn default() -> Self {
        OneImmediatelyImpl {
            counter: Arc::new(AtomicUsize::new(0)),
        }
    }
}
