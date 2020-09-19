use std::sync::atomic::{AtomicUsize, Ordering};
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
            count: AtomicUsize::new(0),
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
        let start_position = self.count.fetch_add(1, Ordering::SeqCst);

        // Try to get one immediatlely and if that fails remeber the
        // error in case we make no futher attempts
        let err = match self
            .try_on_first_checkout_constrait(
                start_position,
                first_checkout_attempt_at,
                CheckoutConstraint::Immediately,
            )
            .await
        {
            Ok(conn) => return Ok(conn),
            Err(err) => err,
        };

        // If the connection was to be checked out immediately (does not allow
        // a reservation), no further
        // attempts may be made and we return the error we already have
        if !constraint.is_reservation_allowed() {
            return Err(err);
        }

        self.try_on_first_checkout_constrait(start_position, first_checkout_attempt_at, constraint)
            .await
    }

    /// Tries all pools until one checks out a connection with the given `CheckoutConstraint`.
    ///
    /// Returns the error of the last attempt if no connection could be checked with
    /// the given constraint on any of the pools
    async fn try_on_first_checkout_constrait(
        &self,
        start_position: usize,
        first_attempt_at: Instant,
        constraint: CheckoutConstraint,
    ) -> Result<Managed<T>, CheckoutError> {
        let mut last_err = CheckoutErrorKind::NoConnection.into();
        for offset in 0..self.pools.len() {
            match get_pool(start_position, offset, &self.pools)
                .check_out_with_timestamp(constraint, first_attempt_at)
                .await
            {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    // Timeot error never happens on Immediate or Wait
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
