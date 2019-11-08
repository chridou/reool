//! Configuration for `RedisPool` including a builder.
//!
//!
//! # Connecting to a single node or multiple replicas
//!
//! ## Connecting to a single node
//!
//! Set the value `connect_to_nodes` to one node only
//!
//! ## Connecting to multiple nodes
//!
//! Set the value `connect_to_nodes` to more than one node.
//! See `NodePoolStrategy` to read on how to configure a pool with multiple
//! nodes.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, warn};

use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::{Instrumentation, InstrumentationFlavour};
use crate::pools::{PoolPerNode, SharedPool};
use crate::redis_rs::RedisRsFactory;

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
    /// Sets the `NodePoolStrategy` to be used when creating the pool.
    pub node_pool_strategy: NodePoolStrategy,
    /// When pool per node is created, sets a multiplier
    /// for the amount of pools per node to be created.
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: (`desired_pool_size`/multiplier) +1
    pub pool_per_node_multiplier: u32,
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

    /// Sets the `NodePoolStrategy` to be used when creating the pool.
    pub fn node_pool_strategy(mut self, v: NodePoolStrategy) -> Self {
        self.node_pool_strategy = v;
        self
    }

    /// When pool per node is created, sets a multiplier
    /// for the amount of pools per node to be created.
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: (`desired_pool_size`/multiplier) +1
    pub fn pool_per_node_multiplier(mut self, v: u32) -> Self {
        self.pool_per_node_multiplier = v;
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
    /// * `NODE_POOL_STRATEGY`: Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
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

        helpers::set_node_pool_strategy(prefix, |v| {
            self.node_pool_strategy = v;
        })?;

        helpers::set_pool_per_node_multiplier(prefix, |v| {
            self.pool_per_node_multiplier = v;
        })?;

        Ok(())
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .checkout_timeout(self.checkout_timeout)
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
            .stats_interval(self.stats_interval)
            .min_required_nodes(self.min_required_nodes)
            .connect_to_nodes(self.connect_to_nodes.clone())
            .node_pool_strategy(self.node_pool_strategy)
            .pool_per_node_multiplier(self.pool_per_node_multiplier)
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
            node_pool_strategy: NodePoolStrategy::default(),
            pool_per_node_multiplier: 1,
        }
    }
}

/// A builder for a `MultiNodePool`
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

    /// Sets the `NodePoolStrategy` to be used when creating the pool.
    pub fn node_pool_strategy(mut self, v: NodePoolStrategy) -> Self {
        self.config.node_pool_strategy = v;
        self
    }

    /// When pool per node is created, sets a multiplier
    /// for the amount of pools per node to be created.
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: (`desired_pool_size`/multiplier) +1
    pub fn pool_per_node_multiplier(mut self, v: u32) -> Self {
        self.config.pool_per_node_multiplier = v;
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
    /// * `CHECKOUT_TIMEOUT_MS`: `u64` or `"NONE"`. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `STATS_INTERVAL_MS`: `u64`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `NODE_POOL_STRATEGY`: ` Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
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
    /// * `NODE_POOL_STRATEGY`: ` Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
    pub fn updated_from_environment(mut self, prefix: Option<&str>) -> InitializationResult<Self> {
        self.config.update_from_environment(prefix)?;
        Ok(self)
    }

    /// Build a new `RedisPool`
    pub fn finish_redis_rs(self) -> InitializationResult<RedisPool> {
        let config = self.config;

        if config.pool_per_node_multiplier == 0 {
            return Err(InitializationError::message_only(
                "pool_per_node_multiplier must not be zero",
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
            warn!("creating a pool with no nodes");
            return Ok(create_no_pool(self.instrumentation));
        }

        let connect_to = config.connect_to_nodes.clone();
        let create_single_pool =
            config.node_pool_strategy == NodePoolStrategy::SharedPool || connect_to.len() == 1;

        let flavour = if create_single_pool {
            debug!(
                "Create shared pool for {} nodes",
                config.connect_to_nodes.len()
            );
            RedisPoolFlavour::Shared(SharedPool::new(
                config,
                RedisRsFactory::new,
                self.executor_flavour,
                self.instrumentation,
            )?)
        } else {
            debug!(
                "Create multiple pools. One for each of the {} nodes",
                config.connect_to_nodes.len()
            );
            RedisPoolFlavour::PerNode(PoolPerNode::new(
                config,
                RedisRsFactory::new,
                self.executor_flavour,
                self.instrumentation,
            )?)
        };

        Ok(RedisPool(flavour))
    }
}

impl std::str::FromStr for NodePoolStrategy {
    type Err = ParseNodesStrategyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "shared-pool" => Ok(NodePoolStrategy::SharedPool),
            "pool-per-node" => Ok(NodePoolStrategy::PoolPerNode),
            "single" | "auto" => {
                 warn!("found 'single' or 'auto' in the env which are deprecated - using 'shared-pool'");
                 Ok(NodePoolStrategy::SharedPool)},
            "multi"  => {
                 warn!("found 'multi' in the env which is deprecated - using 'pool-per-node'");
                Ok(NodePoolStrategy::PoolPerNode)},
            invalid => Err(ParseNodesStrategyError(format!(
                "'{}' is not a valid NodesStrategy. Only 'shared_pool' and 'pool-per-node' are allowed.",
                invalid
            ))),
        }
    }
}

/// Determines which kind of pool to create.
///
/// 2 kinds of pools can be created: A shared pool which
/// will have connections to possibly multiple nodes
/// or a pool with multiple sub pools which will all connect
/// to one node only.
///
/// If there is only 1 node to connect to `NodePoolStrategy::SharedPool` will always
///  be used which is also the default.
///
/// ## `NodePoolStrategy::SharedPool`
///
/// The pool created will create connections to each nose in a round robin fashion.
/// Since connections can also be closed it is never really known how many connections
/// the pool has to a specific node. Nevertheless the connections should be evenly
/// distributed under normal circumstances.
///
/// When a node fails the connections to that node will be replaced by connections to the
/// other nodes once the pool is recreating connections (e.g. after a connection caused an error).
///
/// When pinged it is not certain which od the nodes in the pool is getting pinged.
///
/// In configurations or when using `FromStr` this value is created from the
/// string `shared-pool`.
///
/// ## `NodePoolStrategy::PoolPerNode`
///
/// This pool can only be created if there are more than one node. Each of the pools
/// within the resulting pool will be connected to a single node. When checking out a connection
/// the checkout will be done in a round robin fashion and the next pool will be tried if
/// a pool has no connections left.
///
/// When pinged it is exactly known which host is pinged and it is even guaranteed that all of
/// the hosts will get pinged.
///
/// In configurations or when using `FromStr` this value is created from the
/// string `pool-per-node`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodePoolStrategy {
    /// There will only be a single pool and all connections to maybe different nodes
    /// will share the pool
    SharedPool,
    /// Create a pool that contains connection pools where
    /// each pool is connected to one node only
    PoolPerNode,
}

impl Default for NodePoolStrategy {
    fn default() -> Self {
        NodePoolStrategy::SharedPool
    }
}

#[derive(Debug)]
pub struct ParseNodesStrategyError(String);

impl fmt::Display for ParseNodesStrategyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse NodesStrategy. {}", self.0)
    }
}

impl std::error::Error for ParseNodesStrategyError {
    fn description(&self) -> &str {
        "parse activation order initialization failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

fn create_no_pool(_instrumentation: InstrumentationFlavour) -> RedisPool {
    RedisPool(RedisPoolFlavour::Empty)
}
