//! A connection pool for connecting to a single node
use std::time::Instant;

use futures::prelude::Future;
use log::info;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::InstrumentationFlavour;

use crate::pooled_connection::ConnectionFlavour;
use crate::{Checkout, CheckoutMode, Ping};

use super::pool_internal::{
    instrumentation::PoolInstrumentation, Config as PoolConfig, PoolInternal,
};

/// A connection pool that maintains multiple connections
/// to possibly multiple Redis instances.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub(crate) struct SharedPool {
    pool: PoolInternal<ConnectionFlavour>,
}

impl SharedPool {
    pub fn new<F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: InstrumentationFlavour,
    ) -> InitializationResult<SharedPool>
    where
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(Vec<String>) -> InitializationResult<CF>,
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
            contention_limit: config.contention_limit,
        };

        let connection_factory = if !config.connect_to_nodes.is_empty() {
            create_connection_factory(config.connect_to_nodes.clone())?
        } else {
            return Err(InitializationError::message_only(
                "there is nothing to connect to.",
            ));
        };

        let pool = PoolInternal::new(
            pool_conf,
            connection_factory,
            executor_flavour,
            PoolInstrumentation::new(instrumentation, 0),
        );

        Ok(SharedPool { pool })
    }

    pub fn check_out<M: Into<CheckoutMode>>(&self, mode: M) -> Checkout {
        Checkout(self.pool.check_out(mode.into()))
    }

    pub fn ping(&self, timeout: Instant) -> impl Future<Item = Ping, Error = ()> + Send {
        self.pool.ping(timeout)
    }

    pub fn connected_to(&self) -> &[String] {
        self.pool.connected_to()
    }
}

impl Clone for SharedPool {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}
