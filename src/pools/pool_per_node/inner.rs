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
use crate::pools::pool_internal::{CheckoutManaged, Config as PoolConfig, PoolInternal};
use crate::{Ping, PoolState, Poolable};

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
                "pool_multiplier may not be zero",
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

    pub fn check_out(&self, constraint: CheckoutConstraint) -> CheckoutManaged<T> {
        if self.pools.is_empty() {
            return CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::NoPool,
            )));
        }

        if constraint.is_deadline_elapsed() {
            return CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::CheckoutTimeout,
            )));
        }

        let position = self.count.fetch_add(1, Ordering::SeqCst);
        let first_pool_index = position % self.pools.len();

        // Do the first attempt
        let failed_checkout = match self.pools[first_pool_index].check_out(constraint) {
            Ok(checkout) => return checkout,
            Err(failed_checkout) if self.pools.len() == 1 => {
                return CheckoutManaged::error(failed_checkout.error_kind)
            }
            Err(failed_checkout) => failed_checkout,
        };

        let mut last_failed_checkout = failed_checkout;
        // Iterate over all but the first pool because we already tried that.
        for position in position + 1..position + self.pools.len() {
            if constraint.is_deadline_elapsed() {
                return CheckoutManaged::error(CheckoutErrorKind::CheckoutTimeout);
            }

            let idx = position % self.pools.len();
            let (checkout_package, original_checkout) = last_failed_checkout.explode();

            match self.pools[idx].check_out_package(checkout_package, original_checkout) {
                Ok(checkout) => return checkout,
                Err(failed_checkout) => {
                    last_failed_checkout = failed_checkout;
                }
            }
        }

        CheckoutManaged::error(last_failed_checkout.error_kind)
    }

    pub fn state(&self) -> PoolState {
        self.pools
            .iter()
            .map(|p| p.state())
            .fold(PoolState::default(), |a, b| a + b)
    }

    pub fn ping(&self, timeout: Instant) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        let futs: Vec<_> = self.pools.iter().map(|p| p.ping(timeout)).collect();
        future::join_all(futs)
    }
}
