use std::time::Instant;

use crate::{
    config::CheckoutStrategy, pools::pool_internal::Managed, pools::pool_internal::PoolInternal,
    pools::CheckoutConstraint, CheckoutError, CheckoutErrorKind, Poolable,
};

pub mod one_attempt;
pub mod one_cycle;
pub mod one_immediately;
pub mod two_attempts;
pub mod two_cycles;

impl CheckoutStrategy {
    pub(crate) fn make_impl(self) -> CheckoutStrategyImpl {
        match self {
            CheckoutStrategy::OneAttempt => CheckoutStrategyImpl::OneAttempt(Default::default()),
            CheckoutStrategy::OneCycle => CheckoutStrategyImpl::OneCycle(Default::default()),
            CheckoutStrategy::OneImmediately => {
                CheckoutStrategyImpl::OneImmediately(Default::default())
            }
            CheckoutStrategy::TwoAttempts => CheckoutStrategyImpl::TwoAttempts(Default::default()),
            CheckoutStrategy::TwoCycles => CheckoutStrategyImpl::TwoCycles(Default::default()),
        }
    }
}

impl Default for CheckoutStrategy {
    fn default() -> Self {
        CheckoutStrategy::OneImmediately
    }
}

pub(crate) enum CheckoutStrategyImpl {
    OneAttempt(one_attempt::OneAttemptImpl),
    OneCycle(one_cycle::OneCycleImpl),
    OneImmediately(one_immediately::OneImmediatelyImpl),
    TwoAttempts(two_attempts::TwoAttemptsImpl),
    TwoCycles(two_cycles::TwoCyclesImpl),
}

impl CheckoutStrategyImpl {
    pub async fn apply<T: Poolable>(
        &self,
        constraint: CheckoutConstraint,
        pools: &[PoolInternal<T>],
        first_checkout_attempt_at: Instant,
    ) -> Result<Managed<T>, CheckoutError> {
        match self {
            CheckoutStrategyImpl::OneAttempt(s) => {
                s.apply(constraint, pools, first_checkout_attempt_at).await
            }
            CheckoutStrategyImpl::OneCycle(s) => {
                s.apply(constraint, pools, first_checkout_attempt_at).await
            }
            CheckoutStrategyImpl::OneImmediately(s) => {
                s.apply(constraint, pools, first_checkout_attempt_at).await
            }
            CheckoutStrategyImpl::TwoAttempts(s) => {
                s.apply(constraint, pools, first_checkout_attempt_at).await
            }
            CheckoutStrategyImpl::TwoCycles(s) => {
                s.apply(constraint, pools, first_checkout_attempt_at).await
            }
        }
    }
}

/// Tries all pools until one checks out a connection with the given `CheckoutConstraint`.
///
/// Returns the error of the last attempt if no connection could be checked with
/// the given constraint on any of the pools
async fn one_cycle<T: Poolable>(
    pools: &[PoolInternal<T>],
    start_position: usize,
    initial_offset: usize,
    first_attempt_at: Instant,
    constraint: CheckoutConstraint,
) -> Result<Managed<T>, CheckoutError> {
    let mut last_err = CheckoutErrorKind::NoConnection.into();
    for offset in initial_offset..pools.len() {
        match get_pool(start_position, offset, pools)
            .check_out_with_timestamp(constraint, first_attempt_at)
            .await
        {
            Ok(conn) => return Ok(conn),
            Err(err) => {
                // Timeout error never happens on Immediate or Wait
                if err.kind() == CheckoutErrorKind::CheckoutTimeout {
                    return Err(err);
                }
                last_err = err;
                continue;
            }
        }
    }
    Err(last_err)
}

#[inline]
fn get_pool<T>(start_position: usize, offset: usize, pools: &[T]) -> &T {
    &pools[index_at(start_position, offset, pools.len())]
}

#[inline]
fn index_at(start_position: usize, offset: usize, elements: usize) -> usize {
    (start_position + offset) % elements
}
