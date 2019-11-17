//! A connection pool for connecting to a single node
use std::time::Instant;

use futures::future::{self, Future};
use log::info;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::{InstrumentationFlavour, PoolId};

use crate::{CheckoutMode, Ping, PoolState, Poolable};

use super::pool_internal::{
    instrumentation::PoolInstrumentation, CheckoutManaged, Config as PoolConfig, PoolInternal,
};

/// A connection pool that maintains multiple connections
/// to possibly multiple Redis instances.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub(crate) struct SinglePool<T: Poolable> {
    pool: PoolInternal<T>,
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
            return Err(InitializationError::message_only(
                "'desired_pool_size' must be at least 1",
            ));
        }

        info!(
            "Creating shared pool with {:?} nodes",
            config.connect_to_nodes
        );

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            reservation_limit: config.reservation_limit,
            activation_order: config.activation_order,
            default_checkout_mode: config.default_checkout_mode,
            checkout_queue_size: config.checkout_queue_size,
        };

        let connection_factory = if config.connect_to_nodes.len() != 1 {
            create_connection_factory(config.connect_to_nodes.pop().unwrap())?
        } else {
            return Err(InitializationError::message_only(format!(
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

        Ok(SinglePool { pool })
    }

    pub fn check_out<M: Into<CheckoutMode>>(&self, mode: M) -> CheckoutManaged<T> {
        match self.pool.check_out(mode) {
            Ok(checkout_managed) => checkout_managed,
            Err(error_package) => {
                CheckoutManaged::new(future::err(error_package.error_kind.into()))
            }
        }
    }

    pub fn connected_to(&self) -> &str {
        self.pool.connected_to()
    }

    pub fn state(&self) -> PoolState {
        self.pool.state()
    }

    pub fn ping(&self, timeout: Instant) -> impl Future<Item = Ping, Error = ()> + Send {
        self.pool.ping(timeout)
    }
}

impl<T: Poolable> Clone for SinglePool<T> {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
