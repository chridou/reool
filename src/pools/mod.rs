use std::time::{Duration, Instant};

use futures::future::{self, Future, Loop};
use log::error;
use tokio::timer::Delay;

use crate::{CheckoutError, CheckoutErrorKind, Poolable};

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
/// `CheckoutLimitReached` as long as a retry is allowed
/// by the constraint
pub(crate) fn check_out_maybe_retry_on_queue_limit_reached<P, T, M>(
    pool: &P,
    constraint: M,
    retry_enabled: bool,
) -> CheckoutManaged<T>
where
    P: CanCheckout<T> + Clone + Send + 'static,
    T: Poolable,
    M: Into<CheckoutConstraint>,
{
    if !retry_enabled {
        pool.check_out(constraint)
    } else {
        let pool = pool.clone();
        let constraint = constraint.into();
        CheckoutManaged::new(pool.check_out(constraint).or_else(move |err| {
            if err.kind() != CheckoutErrorKind::CheckoutLimitReached {
                CheckoutManaged::error(err.kind())
            } else {
                retry_on_queue_limit_reached(pool, constraint, err.kind())
            }
        }))
    }
}

fn retry_on_queue_limit_reached<P, T>(
    pool: P,
    constraint: CheckoutConstraint,
    last_err: CheckoutErrorKind,
) -> CheckoutManaged<T>
where
    P: CanCheckout<T> + Send + 'static,
    T: Poolable,
{
    CheckoutManaged::new(future::loop_fn(
        (pool, last_err),
        move |(pool, last_err)| {
            if !constraint.can_wait_for_dispatch() {
                Box::new(future::err(CheckoutError::from(last_err)))
            } else {
                let f = pool.check_out(constraint).then(|r| match r {
                    Ok(conn) => Box::new(future::ok(Loop::Break(conn))),
                    Err(err) => {
                        if err.kind() != CheckoutErrorKind::CheckoutLimitReached {
                            Box::new(future::err(err))
                        } else {
                            let delayed = Delay::new(Instant::now() + Duration::from_millis(1))
                                .map_err(|err| {
                                    error!("A timer error occurred: {}", err);
                                    CheckoutError::new(CheckoutErrorKind::TaskExecution)
                                })
                                .map(move |()| Loop::Continue((pool, err.kind())));
                            Box::new(delayed) as Box<dyn Future<Item = _, Error = _> + Send>
                        }
                    }
                });
                Box::new(f) as Box<dyn Future<Item = _, Error = _> + Send>
            }
        },
    ))
}
