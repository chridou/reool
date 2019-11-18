use futures::future::{self, Future};

use crate::Poolable;
use pool_internal::CheckoutManaged;

mod checkout_constraint;
pub(crate) mod pool_internal;
mod pool_per_node;
mod single_pool;

pub(crate) use self::checkout_constraint::*;
pub(crate) use self::pool_per_node::PoolPerNode;
pub(crate) use self::single_pool::SinglePool;

/// Something that can checkout
pub(crate) trait CanCheckout<T: Poolable> {
    /// Directly do a checkout
    fn check_out<M: Into<CheckoutConstraint>>(&self, constraint: M) -> CheckoutManaged<T>;
}

/// Retry the checkout if the checkout failed with a
/// `QueueLimitReached` as long as a retry is allowed
/// by the constraint
///
/// TODO: Currently only diverts to a regular checkout
pub(crate) fn check_out_retry_on_queue_limit_reached<P, T, M>(
    pool: &P,
    constraint: M,
) -> CheckoutManaged<T>
where
    P: CanCheckout<T> + Clone,
    T: Poolable,
    M: Into<CheckoutConstraint>,
{
    pool.check_out(constraint)
}
