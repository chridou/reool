//! A connection pool for connecting to a single node
use std::time::Duration;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pool_internal::{Config as PoolConfig, PoolInternal};

use crate::pooled_connection::ConnectionFlavour;
use crate::stats::PoolStats;
use crate::Checkout;

/// A connection pool that maintains multiple connections
/// to a single Redis instance.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub(crate) struct SingleNodePool {
    pool: PoolInternal<ConnectionFlavour>,
    checkout_timeout: Option<Duration>,
}

impl SingleNodePool {
    pub(crate) fn create<I, F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<SingleNodePool>
    where
        I: Instrumentation + Send + Sync + 'static,
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(String) -> InitializationResult<CF>,
    {
        if config.desired_pool_size == 0 {
            return Err(InitializationError::message_only(
                "'desired_pool_size' must be at least 1",
            ));
        }

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            reservation_limit: config.reservation_limit,
            stats_interval: config.stats_interval,
            activation_order: config.activation_order,
        };

        let connection_factory = if let Some(connect_to) = config.connect_to_nodes.first() {
            create_connection_factory(connect_to.clone())?
        } else {
            return Err(InitializationError::message_only(
                "There is nothing to connect to.",
            ));
        };

        let pool = PoolInternal::new(
            pool_conf,
            connection_factory,
            executor_flavour,
            instrumentation,
        );

        Ok(SingleNodePool {
            pool,
            checkout_timeout: config.checkout_timeout,
        })
    }

    pub fn check_out(&self) -> Checkout {
        Checkout(self.pool.check_out(self.checkout_timeout))
    }

    pub fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        Checkout(self.pool.check_out(timeout))
    }

    /*
    /// Add `n` new connections to the pool.
    ///
    /// This might not happen immediately.
    /// pub fn add_connections(&self, n: usize) {
    ///     (0..n).for_each(|_| {
    ///         self.pool.add_new_connection();
    ///     });
    /// }

    /// Remove a connection from the pool.
    ///
    /// This might not happen immediately.
    ///
    /// Do not call this function when there are no more connections
    /// managed by the pool. The requests to reduce the
    /// number of connections will are taken from a queue.
    pub fn remove_connection(&self) {
        self.pool.remove_connection();
    }
    */

    /// Get some statistics from the pool.
    ///
    /// This locks the underlying pool.
    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
    }

    /// Triggers the pool to emit statistics if `stats_interval` has elapsed.
    ///
    /// This locks the underlying pool.
    pub fn trigger_stats(&self) {
        self.pool.trigger_stats()
    }
}

impl Clone for SingleNodePool {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
            checkout_timeout: self.checkout_timeout,
        }
    }
}
