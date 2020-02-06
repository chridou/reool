use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future};
use log::{debug, info};

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::{InstrumentationFlavour, PoolId};
use crate::pools::pool_internal::instrumentation::PoolInstrumentation;
use crate::pools::pool_internal::{Config as PoolConfig, PoolInternal, Managed};
use crate::{Ping, PoolState, Poolable, PoolConnection};

use super::super::CheckoutConstraint;

pub(crate) struct Inner<T: Poolable> {
    count: AtomicUsize,
    pub(crate) pools: Arc<Vec<PoolInternal<T>>>,
}

impl<T: Poolable> Inner<T> {
    pub(crate) fn new<F, CF>(
        mut config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: InstrumentationFlavour,
    ) -> InitializationResult<Self>
    where
        CF: ConnectionFactory<Connection = T> + Send + Sync + 'static,
        F: Fn(String) -> InitializationResult<CF>,
    {
        if config.pool_multiplier == 0 {
            return Err(InitializationError::message_only(
                "'pool_multiplier' may not be zero",
            ));
        }

        let multiplier = config.pool_multiplier as usize;
        if multiplier != 1 {
            let new_connections_per_pool = config.desired_pool_size / multiplier + 1;
            let new_reservation_limit = if config.reservation_limit == 0 {
                config.reservation_limit
            } else {
                config.reservation_limit / multiplier + 1
            };

            info!(
                "Pool per node multiplier is {}. Connections per pool will be {}(config: {}) \
                 and the reservation limit will be {}(config: {}).",
                multiplier,
                new_connections_per_pool,
                config.desired_pool_size,
                new_reservation_limit,
                config.reservation_limit
            );

            config.desired_pool_size = new_connections_per_pool;
            config.reservation_limit = new_reservation_limit;
        }

        let mut pools = Vec::new();
        let mut id = PoolId::new(0);
        for _ in 0..multiplier {
            for connect_to in &config.connect_to_nodes {
                let connection_factory = create_connection_factory(connect_to.to_string())?;
                let pool_conf = PoolConfig {
                    desired_pool_size: config.desired_pool_size,
                    backoff_strategy: config.backoff_strategy,
                    reservation_limit: config.reservation_limit,
                    activation_order: config.activation_order,
                    checkout_queue_size: config.checkout_queue_size,
                };

                let indexed_instrumentation = PoolInstrumentation::new(instrumentation.clone(), id);

                let pool = PoolInternal::new(
                    pool_conf,
                    connection_factory,
                    executor_flavour.clone(),
                    indexed_instrumentation,
                );

                pools.push(pool);
                id.inc();
            }
        }

        debug!("pool per node has {} nodes", pools.len());

        let inner = Inner {
            count: AtomicUsize::new(0),
            pools: Arc::new(pools),
        };

        Ok(inner)
    }

    pub async fn check_out(&self, constraint: CheckoutConstraint) -> Result<Managed<T>, CheckoutError> {
        if self.pools.is_empty() {
            return Err(CheckoutErrorKind::NoPool.into());
        }

        if constraint.is_deadline_elapsed() {
            return Err(CheckoutErrorKind::CheckoutTimeout.into());
        }

        let position = self.count.fetch_add(1, Ordering::SeqCst);
        let first_pool_index = position % self.pools.len();

        // Do the first attempt as Immediate since we can still apply original constraint
        // later if we have more then one pool
        let mut last_failed_checkout = {
            let effective_constraint = if self.pools.len() == 1 {
                constraint
            } else {
                CheckoutConstraint::Immediately
            };

            match self.pools[first_pool_index].check_out(effective_constraint).await {
                Ok(checkout) => return Ok(checkout),
                Err(failed_checkout) if self.pools.len() == 1 => {
                    return Err(failed_checkout.error_kind.into());
                }
                Err(failed_checkout) => failed_checkout,
            }
        };

        let iteration_bound = position + self.pools.len();
        let last_iteration = iteration_bound - 1;
        // Iterate over all but the first pool because we already tried that.
        for position in position + 1..iteration_bound {
            if constraint.is_deadline_elapsed() {
                return Err(CheckoutErrorKind::CheckoutTimeout.into());
            }

            let idx = position % self.pools.len();

            let current_constraint = if position >= last_iteration {
                constraint
            } else {
                CheckoutConstraint::Immediately
            };

            match self.pools[idx].check_out2(
                last_failed_checkout.checkout_requested_at,
                current_constraint,
            ).await {
                Ok(checkout) => return Ok(checkout),
                Err(failed_checkout) => {
                    last_failed_checkout = failed_checkout;
                }
            }
        }

        Err(last_failed_checkout.error_kind.into())
    }

    pub fn state(&self) -> PoolState {
        self.pools
            .iter()
            .map(|p| p.state())
            .fold(PoolState::default(), |a, b| a + b)
    }

    pub fn ping<'a>(&'a self, timeout: Instant) -> impl Future<Output = Vec<Ping>> + Send + 'a {
        let futs = self.pools.iter().map(|p| p.ping(timeout));
        future::join_all(futs)
    }
}
