use std::time::Duration;

use futures::future::BoxFuture;
use tokio::time::delay_for;

use crate::{CheckoutError, CheckoutErrorKind, Poolable};

use pool_internal::Managed;

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
    fn check_out<'a, M: Into<CheckoutConstraint> + Send + 'static>(&'a self, constraint: M) -> BoxFuture<'a, Result<Managed<T>, CheckoutError>>;
}

/// Retry the checkout if the checkout failed with a
/// `CheckoutLimitReached` as long as a retry is allowed
/// by the constraint
pub(crate) async fn check_out_maybe_retry_on_queue_limit_reached<P, T, M>(
    pool: &P,
    constraint: M,
    retry_enabled: bool,
) -> Result<Managed<T>, CheckoutError>
where
    P: CanCheckout<T> + Clone + Send + 'static,
    T: Poolable,
    M: Into<CheckoutConstraint> + Send + 'static,
{
    if !retry_enabled {
        return pool.check_out(constraint).await;
    }

    let constraint = constraint.into();

    match pool.check_out(constraint).await {
        Ok(conn) => Ok(conn),
        Err(err) => {
            if err.kind() != CheckoutErrorKind::CheckoutLimitReached {
                Err(err)
            } else {
                retry_on_queue_limit_reached(pool, constraint, err.kind()).await
            }
        }
    }
}

async fn retry_on_queue_limit_reached<P, T>(
    pool: &P,
    constraint: CheckoutConstraint,
    last_err: CheckoutErrorKind,
) -> Result<Managed<T>, CheckoutError>
where
    P: CanCheckout<T> + Send + 'static,
    T: Poolable,
{
    loop {
        if !constraint.can_wait_for_dispatch() {
            return Err(CheckoutError::from(last_err));
        }

        match pool.check_out(constraint).await {
            Ok(conn) => return Ok(conn),
            Err(err) => {
                if err.kind() != CheckoutErrorKind::CheckoutLimitReached {
                    return Err(err);
                }

                delay_for(Duration::from_millis(1)).await;
            }
        }
    }
}
