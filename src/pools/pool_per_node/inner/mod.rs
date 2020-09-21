use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future};
use log::{debug, info};

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::error::{Error, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::{InstrumentationFlavour, PoolId};
use crate::pools::pool_internal::instrumentation::PoolInstrumentation;
use crate::pools::pool_internal::{Config as PoolConfig, Managed, PoolInternal};
use crate::{Ping, PoolState, Poolable};

use super::super::CheckoutConstraint;

mod checkout_strategies;

pub(crate) struct Inner<T: Poolable> {
    strategy: checkout_strategies::CheckoutStrategyImpl,
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
            return Err(Error::message("'pool_multiplier' may not be zero"));
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
            strategy: config.checkout_strategy.make_impl(),
            pools: Arc::new(pools),
        };

        Ok(inner)
    }

    pub async fn check_out(
        &self,
        constraint: CheckoutConstraint,
    ) -> Result<Managed<T>, CheckoutError> {
        if self.pools.is_empty() {
            return Err(CheckoutErrorKind::NoPool.into());
        }

        let first_checkout_attempt_at = Instant::now();
        self.strategy
            .apply(constraint, &self.pools, first_checkout_attempt_at)
            .await
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

#[inline]
fn get_pool<T>(start_position: usize, offset: usize, pools: &[T]) -> &T {
    &pools[index_at(start_position, offset, pools.len())]
}

#[inline]
fn index_at(start_position: usize, offset: usize, elements: usize) -> usize {
    (start_position + offset) % elements
}
