use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future;
use log::{debug, warn};

use crate::connection_factory::ConnectionFactory;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::multi_node_pool::instrumentation::*;
use crate::pool_internal::{CheckoutManaged, Config as PoolConfig, PoolInternal};
use crate::stats::PoolStats;
use crate::{Checkout, Poolable};

use super::Config;

pub struct Inner<T: Poolable> {
    count: AtomicUsize,
    pools: Vec<PoolInternal<T>>,
    checkout_timeout: Option<Duration>,
}

impl<T: Poolable + redis::r#async::ConnectionLike> Inner<T> {
    pub(crate) fn new<I, F>(
        config: Config,
        connection_factories: Vec<F>,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<Self>
    where
        I: Instrumentation + Send + Sync + 'static,
        F: ConnectionFactory<Connection = T> + Send + Sync + 'static,
    {
        let mut pools = Vec::new();

        let instrumentation_aggregator = instrumentation
            .map(InstrumentationAggregator::new)
            .map(Arc::new);

        let mut pool_idx = 0;
        for connection_factory in connection_factories {
            let pool_conf = PoolConfig {
                desired_pool_size: config.desired_pool_size,
                backoff_strategy: config.backoff_strategy,
                reservation_limit: config.reservation_limit,
                stats_interval: config.stats_interval,
                activation_order: config.activation_order,
            };

            let indexed_instrumentation = instrumentation_aggregator.as_ref().map(|agg| {
                let instr = IndexedInstrumentation::new(agg.clone(), pool_idx);
                agg.add_new_pool();
                instr
            });

            let pool = PoolInternal::new(
                pool_conf,
                connection_factory,
                executor_flavour.clone(),
                indexed_instrumentation,
            );

            pools.push(pool);

            pool_idx += 1;
        }

        if pools.len() < config.min_required_nodes {
            return Err(InitializationError::message_only(format!(
                "the minimum required nodes is {} but there are only {}",
                config.min_required_nodes,
                pools.len()
            )));
        } else if pools.is_empty() {
            warn!("no nodes is allowed and there are no nodes.");
        }

        debug!("replica set has {} nodes", pools.len());

        Ok(Self {
            pools,
            count: AtomicUsize::new(0),
            checkout_timeout: config.checkout_timeout,
        })
    }

    pub fn check_out(&self) -> Checkout<T> {
        self.check_out_explicit_timeout(self.checkout_timeout)
    }

    pub fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout<T> {
        if self.pools.is_empty() {
            Checkout(CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::NoConnection,
            ))))
        } else {
            let count = self.count.fetch_add(1, Ordering::SeqCst);

            let idx = count % self.pools.len();

            Checkout(self.pools[idx].check_out(timeout))
        }
    }

    pub fn stats(&self) -> Vec<PoolStats> {
        self.pools.iter().map(PoolInternal::stats).collect()
    }

    pub fn trigger_stats(&self) {
        self.pools.iter().for_each(PoolInternal::trigger_stats)
    }
}
