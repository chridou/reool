//! Configuration for `RedisPool` including a builder.
//!
//!
//! # Connecting to a single node or multiple replicas
//!
//! ## Connecting to a single node
//!
//! Set the value `connect_to_nodes` to one node or explicitly set
//! `PoolMode::Single` in which case the first of multiple node
//! will be selected.
//!
//! ## Connecting to a multiple nodes
//!
//! Set the value `connect_to_nodes` to more than one nodes or explicitly set
//! `PoolMode::Multi` if there is only a single node to connect to but you
//! want to enforce the mechanics for a replica set pool.
use std::fmt;
use std::time::Duration;

use log::{debug, warn};

use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::{Instrumentation, NoInstrumentation};
use crate::multi_node_pool::MultiNodePool;
use crate::redis_rs::RedisRsFactory;
use crate::single_node_pool::SingleNodePool;

pub use crate::activation_order::ActivationOrder;
pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::error::InitializationError;

use super::{RedisPool, RedisPoolFlavour};

/// A configuration for creating a `MultiNodePool`.
///
/// You should prefer using the `MultiNodePool::builder()` function.
#[derive(Debug, Clone)]
pub struct Config {
    /// The number of connections the pool should initially have
    /// and try to maintain
    pub desired_pool_size: usize,
    /// The timeout for a checkout if no specific timeout is given
    /// with a checkout.
    pub checkout_timeout: Option<Duration>,
    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub backoff_strategy: BackoffStrategy,
    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub reservation_limit: Option<usize>,
    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub stats_interval: Duration,
    /// Defines the `ActivationOrder` in which idle connections are
    /// activated.
    ///
    /// Default is `ActivationOrder::FiFo`
    pub activation_order: ActivationOrder,
    /// The minimum required nodes to start
    pub min_required_nodes: usize,
    /// The nodes to connect To
    pub connect_to_nodes: Vec<String>,
    /// Sets the `PoolMode` to be used when creating the pool.
    pub pool_mode: PoolMode,
}

impl Config {
    /// Sets the number of connections the pool should initially have
    /// and try to maintain
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.desired_pool_size = v;
        self
    }

    /// Sets the timeout for a checkout if no specific timeout is given
    /// with a checkout.
    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.checkout_timeout = v;
        self
    }

    /// Sets the `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.backoff_strategy = v;
        self
    }

    /// Sets the maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn reservation_limit(mut self, v: Option<usize>) -> Self {
        self.reservation_limit = v;
        self
    }

    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub fn stats_interval(mut self, v: Duration) -> Self {
        self.stats_interval = v;
        self
    }

    /// Defines the `ActivationOrder` in which idle connections are
    /// activated.
    ///
    /// Default is `ActivationOrder::FiFo`
    pub fn activation_order(mut self, v: ActivationOrder) -> Self {
        self.activation_order = v;
        self
    }

    /// Sets the maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn min_required_nodes(mut self, v: usize) -> Self {
        self.min_required_nodes = v;
        self
    }

    /// The Redis nodes to connect to
    pub fn connect_to_nodes(mut self, v: Vec<String>) -> Self {
        self.connect_to_nodes = v;
        self
    }

    /// The Redis node to connect to
    pub fn connect_to_node<T: Into<String>>(mut self, v: T) -> Self {
        self.connect_to_nodes = vec![v.into()];
        self
    }

    /// Sets the `PoolMode` to be used when creating the pool.
    pub fn pool_mode(mut self, v: PoolMode) -> Self {
        self.pool_mode = v;
        self
    }

    /// Updates this configuration from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `POOL_MODE`: Omit if you do not want to update the value
    pub fn update_from_environment(&mut self, prefix: Option<&str>) -> InitializationResult<()> {
        helpers::set_desired_pool_size(prefix, |v| {
            self.desired_pool_size = v;
        })?;

        helpers::set_checkout_timeout(prefix, |v| {
            self.checkout_timeout = v;
        })?;

        helpers::set_reservation_limit(prefix, |v| {
            self.reservation_limit = v;
        })?;

        helpers::set_stats_interval(prefix, |v| {
            self.stats_interval = v;
        })?;

        helpers::set_activation_order(prefix, |v| {
            self.activation_order = v;
        })?;

        helpers::set_min_required_nodes(prefix, |v| {
            self.min_required_nodes = v;
        })?;

        if let Some(v) = helpers::get_connect_to(prefix)? {
            self.connect_to_nodes = v;
        };

        helpers::set_pool_mode(prefix, |v| {
            self.pool_mode = v;
        })?;

        Ok(())
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder<NoInstrumentation> {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .checkout_timeout(self.checkout_timeout)
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
            .stats_interval(self.stats_interval)
            .min_required_nodes(self.min_required_nodes)
            .connect_to_nodes(self.connect_to_nodes.clone())
            .pool_mode(self.pool_mode)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            checkout_timeout: Some(Duration::from_millis(20)),
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: Some(100),
            stats_interval: Duration::from_millis(100),
            activation_order: ActivationOrder::default(),
            min_required_nodes: 1,
            connect_to_nodes: Vec::new(),
            pool_mode: PoolMode::default(),
        }
    }
}

/// A builder for a `MultiNodePool`
pub struct Builder<I> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    instrumentation: Option<I>,
}

impl Default for Builder<NoInstrumentation> {
    fn default() -> Self {
        Self {
            config: Config::default(),
            executor_flavour: ExecutorFlavour::Runtime,
            instrumentation: None,
        }
    }
}

impl<I> Builder<I> {
    /// The number of connections the pool should initially have
    /// and try to maintain
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.config.desired_pool_size = v;
        self
    }

    /// The timeout for a checkout if no specific timeout is given
    /// with a checkout.
    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.config.checkout_timeout = v;
        self
    }

    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.config.backoff_strategy = v;
        self
    }

    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub fn reservation_limit(mut self, v: Option<usize>) -> Self {
        self.config.reservation_limit = v;
        self
    }

    /// The interval in which the pool will send statistics to
    /// the instrumentation
    pub fn stats_interval(mut self, v: Duration) -> Self {
        self.config.stats_interval = v;
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

    /// Sets the `PoolMode` to be used when creating the pool.
    pub fn pool_mode(mut self, v: PoolMode) -> Self {
        self.config.pool_mode = v;
        self
    }

    /// The executor to use for spawning tasks. If not set it is assumed
    /// that the pool is created on the default runtime.
    pub fn task_executor(mut self, executor: ::tokio::runtime::TaskExecutor) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioTaskExecutor(executor);
        self
    }

    /// Adds instrumentation to the pool
    pub fn instrumented<II>(self, instrumentation: II) -> Builder<II>
    where
        II: Instrumentation + Send + Sync + 'static,
    {
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            instrumentation: Some(instrumentation),
        }
    }

    #[cfg(feature = "metrix")]
    pub fn instrumented_with_metrix<A: metrix::processor::AggregatesProcessors>(
        self,
        aggregates_processors: &mut A,
        config: crate::instrumentation::MetrixConfig,
    ) -> Builder<crate::instrumentation::metrix::MetrixInstrumentation> {
        let instrumentation = crate::instrumentation::metrix::create(aggregates_processors, config);
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            instrumentation: Some(instrumentation),
        }
    }

    /// Sets values in this builder from the environment.
    ///
    /// If no `prefix` is set all the given env key start with `REOOL_`.
    /// Otherwise the prefix is used with an automatically appended `_`.
    ///
    /// * `DESIRED_POOL_SIZE`: `usize`. Omit if you do not want to update the value
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `POOL_MODE`: ` Omit if you do not want to update the value
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
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `POOL_MODE`: ` Omit if you do not want to update the value
    pub fn updated_from_environment(mut self, prefix: Option<&str>) -> InitializationResult<Self> {
        self.config.update_from_environment(prefix)?;
        Ok(self)
    }
}

impl<I> Builder<I>
where
    I: Instrumentation + Send + Sync + 'static,
{
    /// Build a new `RedisPool`
    pub fn redis_rs(self) -> InitializationResult<RedisPool> {
        if self.config.connect_to_nodes.len() < self.config.min_required_nodes {
            return Err(InitializationError::message_only(format!(
                "There must be at least {} node(s) defined. There are only {} defined.",
                self.config.min_required_nodes,
                self.config.connect_to_nodes.len()
            )));
        }

        if self.config.connect_to_nodes.is_empty() {
            warn!("creating a pool with no nodes");
            return Ok(create_no_pool(self.instrumentation));
        }

        let create_single_pool = self.config.pool_mode == PoolMode::Single
            || (self.config.pool_mode == PoolMode::Auto && self.config.connect_to_nodes.len() == 1);

        let flavour = if create_single_pool {
            debug!("Create pool with single node");
            RedisPoolFlavour::SingleNode(SingleNodePool::create(
                self.config,
                RedisRsFactory::new,
                self.executor_flavour,
                self.instrumentation,
            )?)
        } else {
            debug!("Create pool with multiple nodes");
            RedisPoolFlavour::MultiNode(MultiNodePool::create(
                self.config,
                RedisRsFactory::new,
                self.executor_flavour,
                self.instrumentation,
            )?)
        };

        Ok(RedisPool(flavour))
    }
}

impl std::str::FromStr for PoolMode {
    type Err = ParsePoolModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "single" => Ok(PoolMode::Single),
            "multi" => Ok(PoolMode::Multi),
            "auto" => Ok(PoolMode::Auto),
            invalid => Err(ParsePoolModeError(format!(
                "'{}' is not a valid PoolMode. Only 'single' and 'multi' and 'auto' are allowed.",
                invalid
            ))),
        }
    }
}

/// Determines which kind of pool to create.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PoolMode {
    /// Create the pool depending on the number of nodes specified
    /// via the `connect_to_*`-parameters:
    ///
    /// * One node: Create a pool for a single node
    /// * Two or more: Create a multi node pool
    Auto,
    /// Always create a single node pool
    Single,
    /// Always create a multi node pool
    Multi,
}

impl Default for PoolMode {
    fn default() -> Self {
        PoolMode::Auto
    }
}

#[derive(Debug)]
pub struct ParsePoolModeError(String);

impl fmt::Display for ParsePoolModeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse PoolMode. {}", self.0)
    }
}

impl std::error::Error for ParsePoolModeError {
    fn description(&self) -> &str {
        "parse activation order initialization failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

fn create_no_pool<I>(instrumentation: Option<I>) -> RedisPool
where
    I: Instrumentation,
{
    instrumentation
        .iter()
        .for_each(|instr| instr.stats(crate::stats::PoolStats::default()));
    RedisPool(RedisPoolFlavour::NoPool)
}
