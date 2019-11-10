use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::{self, Future, Loop};
use log::{debug, info, warn};

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{CheckoutError, CheckoutErrorKind};
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::InstrumentationFlavour;
use crate::pooled_connection::ConnectionFlavour;
use crate::pools::pool_internal::instrumentation::PoolInstrumentation;
use crate::pools::pool_internal::{CheckoutManaged, Config as PoolConfig, PoolInternal};
use crate::{Checkout, CheckoutMode, Ping};

pub struct Inner {
    count: AtomicUsize,
    pub(crate) pools: Arc<Vec<PoolInternal<ConnectionFlavour>>>,
    pub(crate) connected_to: Vec<String>,
}

impl Inner {
    pub(crate) fn new<F, CF>(
        mut config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: InstrumentationFlavour,
    ) -> InitializationResult<Self>
    where
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(Vec<String>) -> InitializationResult<CF>,
    {
        if config.pool_per_node_multiplier == 0 {
            return Err(InitializationError::message_only(
                "pool_per_node_multiplier may not be zero",
            ));
        }

        let connect_to_distinct = config.connect_to_nodes.clone();

        let multiplier = config.pool_per_node_multiplier as usize;
        if multiplier != 1 {
            info!("pool per node multiplier is {}", multiplier);
            if let Some(ref mut limit) = config.reservation_limit {
                let new_limit = *limit / multiplier + 1;
                info!("new reservation limit is is {}", new_limit);
                *limit = new_limit;
            }

            let new_connections_per_pool = config.desired_pool_size / multiplier + 1;
            info!("connections per pool is {}", new_connections_per_pool);
            config.desired_pool_size = new_connections_per_pool;
        }

        let mut pools = Vec::new();
        let mut pool_index = 0;
        for _ in 0..multiplier {
            for connect_to in &config.connect_to_nodes {
                let connection_factory = create_connection_factory(vec![connect_to.to_string()])?;
                let pool_conf = PoolConfig {
                    desired_pool_size: config.desired_pool_size,
                    checkout_mode: config.checkout_mode,
                    backoff_strategy: config.backoff_strategy,
                    reservation_limit: config.reservation_limit,
                    activation_order: config.activation_order,
                };

                let indexed_instrumentation =
                    PoolInstrumentation::new(instrumentation.clone(), pool_index);

                let pool = PoolInternal::new(
                    pool_conf,
                    connection_factory,
                    executor_flavour.clone(),
                    indexed_instrumentation,
                );

                pools.push(pool);
                pool_index += 1;
            }
        }

        debug!("pool per node has {} nodes", pools.len());

        let inner = Inner {
            count: AtomicUsize::new(0),
            pools: Arc::new(pools),
            connected_to: connect_to_distinct,
        };

        Ok(inner)
    }

    pub fn check_out(&self, mode: CheckoutMode) -> Checkout {
        if self.pools.is_empty() {
            Checkout(CheckoutManaged::new(future::err(CheckoutError::new(
                CheckoutErrorKind::NoPool,
            ))))
        } else {
            let count = self.count.fetch_add(1, Ordering::SeqCst);

            // If a pool fails to checkout a connection try the next one.
            //
            // Start with the checkout mode passed by the user. On a failure
            // continue with CheckoutMode::Immediately.
            let loop_fut = future::loop_fn(
                (Arc::clone(&self.pools), self.pools.len(), mode),
                move |(pools, attempts_left, mode)| {
                    if attempts_left == 0 {
                        return Box::new(future::err(CheckoutErrorKind::NoConnection.into()))
                            as Box<dyn Future<Item = _, Error = CheckoutError> + Send>;
                    }

                    let idx = (count + attempts_left) % pools.len();

                    Box::new(pools[idx].check_out(mode).then(move |r| match r {
                        Ok(managed_conn) => Ok(Loop::Break(managed_conn)),
                        Err(err) => {
                            warn!("no connection from pool - trying next - {}", err);
                            Ok(Loop::Continue((
                                pools,
                                attempts_left - 1,
                                CheckoutMode::Immediately,
                            )))
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
