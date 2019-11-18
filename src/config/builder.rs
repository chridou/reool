use std::sync::Arc;

use log::{debug, info, warn};

use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::{Instrumentation, InstrumentationFlavour};
use crate::pools::{PoolPerNode, SinglePool};
use crate::redis_rs::RedisRsFactory;
use crate::{RedisPool, RedisPoolFlavour};

use super::*;

pub use crate::activation_order::ActivationOrder;
pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::error::InitializationError;

/// A builder for a `RedisPool`
pub struct Builder {
    config: Config,
    executor_flavour: ExecutorFlavour,
    instrumentation: InstrumentationFlavour,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            config: Config::default(),
            executor_flavour: ExecutorFlavour::Runtime,
            instrumentation: InstrumentationFlavour::NoInstrumentation,
        }
    }
}

impl Builder {
    /// The number of connections a pool should have. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.config.desired_pool_size = v;
        self
    }

    /// Sets the behaviour of the pool on checkouts if no specific behaviour
    /// was requested by the user.
    pub fn default_checkout_mode<T: Into<DefaultPoolCheckoutMode>>(mut self, v: T) -> Self {
        self.config.default_checkout_mode = v.into().adjust();
        self
    }

    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.config.backoff_strategy = v;
        self
    }

    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
    pub fn reservation_limit(mut self, v: usize) -> Self {
        self.config.reservation_limit = v;
        self
    }

    pub fn activation_order(mut self, v: ActivationOrder) -> Self {
        self.config.activation_order = v;
        self
    }

    /// The minimum required nodes to start
    pub fn min_required_nodes(mut self, v: usize) -> Self {
        self.config.min_required_nodes = v;
        self
    }

    /// The Redis nodes to connect to
    pub fn connect_to_nodes(mut self, v: Vec<String>) -> Self {
        self.config.connect_to_nodes = v;
        self
    }

    /// The Redis node to connect to
    pub fn connect_to_node<T: Into<String>>(mut self, v: T) -> Self {
        self.config.connect_to_nodes = vec![v.into()];
        self
    }

    /// When the pool is created this is a multiplier for the amount of sub
    /// pools to be created.
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: `desired_pool_size`/multiplier) +1
    pub fn pool_multiplier(mut self, v: u32) -> Self {
        self.config.pool_multiplier = v;
        self
    }

    /// The number of checkouts that can be enqueued. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 100.
    pub fn checkout_queue_size(mut self, v: usize) -> Self {
        self.config.checkout_queue_size = v;
        self
    }

    /// Set to `true` if a retry on a checkout should be made if the queue was full.
    /// Otherwise do not retry.
    ///
    /// The default is `true`.
    pub fn retry_on_checkout_limit(mut self, v: bool) -> Self {
        self.config.retry_on_checkout_limit = v;
        self
    }

    /// The executor to use for spawning tasks. If not set it is assumed
    /// that the pool is created on the default runtime.
    pub fn task_executor(mut self, executor: ::tokio::runtime::TaskExecutor) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioTaskExecutor(executor);
        self
    }

    /// Adds instrumentation to the pool
    pub fn instrumented<I>(mut self, instrumentation: I) -> Self
    where
        I: Instrumentation + Send + Sync + 'static,
    {
        self.instrumentation = InstrumentationFlavour::Custom(Arc::new(instrumentation));
        self
    }

    #[cfg(feature = "metrix")]
    pub fn with_mounted_metrix_instrumentation<A: metrix::processor::AggregatesProcessors>(
        mut self,
        aggregates_processors: &mut A,
        config: crate::instrumentation::MetrixConfig,
    ) -> Self {
        let instrumentation =
            crate::instrumentation::MetrixInstrumentation::new(aggregates_processors, config);
        self.instrumentation = InstrumentationFlavour::Metrix(instrumentation);
        self
    }

    #[cfg(feature = "metrix")]
    pub fn with_metrix_instrumentation(
        mut self,
        instrumentation: crate::instrumentation::MetrixInstrumentation,
    ) -> Self {
        self.instrumentation = InstrumentationFlavour::Metrix(instrumentation);
        self
    }

    /// Sets values in this builder from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `DEFAULT_POOL_CHECKOUT_MODE`: The default checkout mode to use. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `POOL_MULTIPLIER`: Omit if you do not want to update the value
    /// * `CHECKOUT_QUEUE_SIZE`: Omit if you do not want to update the value
    /// * `RETRY_ON_CHECKOUT_LIMIT`: Omit if you do not want to update the value
    pub fn update_from_environment(&mut self, prefix: Option<&str>) -> InitializationResult<()> {
        self.config.update_from_environment(prefix)?;
        Ok(())
    }

    /// Updates this builder from the environment and returns `Self`.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `DEFAULT_POOL_CHECKOUT_MODE`: The default checkout mode to use. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `POOL_MULTIPLIER`: Omit if you do not want to update the value
    /// * `CHECKOUT_QUEUE_SIZE`: Omit if you do not want to update the value
    /// * `RETRY_ON_CHECKOUT_LIMIT`: Omit if you do not want to update the value
    pub fn updated_from_environment(mut self, prefix: Option<&str>) -> InitializationResult<Self> {
        self.config.update_from_environment(prefix)?;
        Ok(self)
    }

    /// Build a new `RedisPool` with the given connection factory
    pub fn finish<CF, F>(
        self,
        connection_factory: F,
    ) -> InitializationResult<RedisPool<CF::Connection>>
    where
        F: Fn(String) -> InitializationResult<CF>,
        CF: ConnectionFactory + Send + Sync + 'static,
    {
        let config = self.config;

        if config.pool_multiplier == 0 {
            return Err(InitializationError::message_only(
                "pool_multiplier must not be zero",
            ));
        }

        if config.checkout_queue_size == 0 {
            return Err(InitializationError::message_only(
                "checkout_queue_size must be greater than 0",
            ));
        }

        if config.connect_to_nodes.len() < config.min_required_nodes {
            return Err(InitializationError::message_only(format!(
                "There must be at least {} node(s) defined. There are only {} defined.",
                config.min_required_nodes,
                config.connect_to_nodes.len()
            )));
        }

        if config.connect_to_nodes.is_empty() {
            warn!("Creating a pool with no nodes");
            return Ok(create_no_pool(self.instrumentation));
        }

        info!("Configuration: {:?}", config);

        let create_single_pool = config.connect_to_nodes.len() == 1 && config.pool_multiplier == 1;

        let default_checkout_mode = config.default_checkout_mode;
        let retry_on_checkout_limit = config.retry_on_checkout_limit;

        let flavour = if create_single_pool {
            debug!("Create single pool for 1 node",);

            RedisPoolFlavour::Single(SinglePool::new(
                config,
                connection_factory,
                self.executor_flavour,
                self.instrumentation,
            )?)
        } else {
            debug!(
                "Create multiple pools. One for each of  the {} nodes",
                config.connect_to_nodes.len()
            );
            RedisPoolFlavour::PerNode(PoolPerNode::new(
                config,
                connection_factory,
                self.executor_flavour,
                self.instrumentation,
            )?)
        };

        Ok(RedisPool {
            flavour,
            default_checkout_mode,
            retry_on_checkout_limit,
        })
    }

    /// Build a new `RedisPool`
    pub fn finish_redis_rs(self) -> InitializationResult<RedisPool> {
        self.finish(RedisRsFactory::new)
    }
}

fn create_no_pool<T: Poolable>(_instrumentation: InstrumentationFlavour) -> RedisPool<T> {
    RedisPool::no_pool()
}
