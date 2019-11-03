use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{self, Future, Loop};
use log::{debug, warn};

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationResult;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pooled_connection::ConnectionFlavour;
use crate::pools::pool_internal::{CheckoutManaged, Config as PoolConfig, PoolInternal};
use crate::{Checkout, Ping};

use super::instrumentation::*;

pub struct Inner {
    count: AtomicUsize,
    pub(crate) pools: Arc<Vec<PoolInternal<ConnectionFlavour>>>,
}

impl Inner {
    pub(crate) fn new<I, F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<Self>
    where
        I: Instrumentation + Send + Sync + 'static,
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(Vec<String>) -> InitializationResult<CF>,
    {
        let mut pools = Vec::new();

        let instrumentation_aggregator = instrumentation
            .map(InstrumentationAggregator::new)
            .map(Arc::new);

        let mut pool_idx = 0;
        for connect_to in config.connect_to_nodes {
            let connection_factory = create_connection_factory(vec![connect_to])?;
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

        debug!("pool per node has {} nodes", pools.len());

        let inner = Inner {
            count: AtomicUsize::new(0),
            pools: Arc::new(pools),
        };

        Ok(inner)
    }

    pub fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        if self.pools.is_empty() {
            Checkout(CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::NoPool,
            ))))
        } else {
            let count = self.count.fetch_add(1, Ordering::SeqCst);

            let loop_fut = future::loop_fn(
                (Arc::clone(&self.pools), self.pools.len()),
                move |(pools, attempts_left)| {
                    if attempts_left == 0 {
                        return Box::new(future::err(CheckoutErrorKind::NoConnection.into()))
                            as Box<dyn Future<Item = _, Error = CheckoutError> + Send>;
                    }

                    let idx = count % pools.len();

                    Box::new(pools[idx].check_out(timeout).then(move |r| match r {
                        Ok(managed_conn) => Ok(Loop::Break(managed_conn)),
                        Err(err) => {
                            warn!("no connection from pool - trying next - {}", err);
                            Ok(Loop::Continue((pools, attempts_left - 1)))
                        }
                    }))
                },
            );

            Checkout::new(loop_fut)
        }
    }

    pub fn ping(&self, timeout: Duration) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        let futs: Vec<_> = self.pools.iter().map(|p| p.ping(timeout)).collect();
        future::join_all(futs)
    }
}