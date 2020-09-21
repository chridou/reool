//! A connection pool for connecting to a single node
use std::sync::Arc;
use std::time::Instant;

use future::BoxFuture;
use futures::prelude::*;
use log::info;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{Error, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::{InstrumentationFlavour, PoolId};

use crate::{CheckoutError, Ping, PoolState, Poolable};

use super::{CanCheckout, CheckoutConstraint};

use super::pool_internal::{
    instrumentation::PoolInstrumentation, Config as PoolConfig, Managed, PoolInternal,
};

/// A connection pool that maintains multiple connections
/// to one node only.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub(crate) struct SinglePool<T: Poolable> {
    pool: Arc<PoolInternal<T>>,
}

impl<T: Poolable> SinglePool<T> {
    pub fn new<F, CF>(
        mut config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: InstrumentationFlavour,
    ) -> InitializationResult<SinglePool<T>>
    where
        CF: ConnectionFactory<Connection = T> + Send + Sync + 'static,
        F: Fn(String) -> InitializationResult<CF>,
    {
        if config.desired_pool_size == 0 {
            return Err(Error::message("'desired_pool_size' must be at least one"));
        }

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            reservation_limit: config.reservation_limit,
            activation_order: config.activation_order,
            checkout_queue_size: config.checkout_queue_size,
        };

        let connection_factory = if config.connect_to_nodes.len() == 1 {
            let connect_to = config.connect_to_nodes.pop().unwrap();
            info!(
                "Creating pool for '{}' with {} connections",
                connect_to, config.desired_pool_size,
            );

            create_connection_factory(connect_to)?
        } else {
            return Err(Error::message(format!(
                "there must be exactly 1 connection string given - found {}",
                config.connect_to_nodes.len()
            )));
        };

        let pool = PoolInternal::new(
            pool_conf,
            connection_factory,
            executor_flavour,
            PoolInstrumentation::new(instrumentation, PoolId::new(0)),
        );

        Ok(SinglePool {
            pool: Arc::new(pool),
        })
    }

    pub fn connected_to(&self) -> &str {
        self.pool.connected_to()
    }

    pub fn state(&self) -> PoolState {
        self.pool.state()
    }

    pub fn ping(&self, timeout: Instant) -> BoxFuture<Ping> {
        self.pool.ping(timeout)
    }
}

impl<T: Poolable> CanCheckout<T> for SinglePool<T> {
    fn check_out<'a, M: Into<CheckoutConstraint> + Send + 'static>(
        &'a self,
        constraint: M,
    ) -> BoxFuture<'a, Result<Managed<T>, CheckoutError>> {
        self.pool.check_out(constraint).boxed()
    }
}

impl<T: Poolable> Clone for SinglePool<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
