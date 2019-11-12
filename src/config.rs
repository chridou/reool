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

use super::{Immediately, RedisPool, RedisPoolFlavour, Wait};

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
    pub default_checkout_mode: DefaultPoolCheckoutMode,
    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub backoff_strategy: BackoffStrategy,
    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available
    pub reservation_limit: Option<usize>,
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
    /// Increasing this values reduces the contention on each created pool
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: (`desired_pool_size`/multiplier) +1
    pub pool_per_node_multiplier: u32,
    /// The contention limit of the pool. If a pool per node is
    /// configured the limit is for each inner pool.ContentionLimit
    ///
    /// The default is `NoLimit`
    pub contention_limit: ContentionLimit,
}

impl Config {
    /// Sets the number of connections the pool should initially have
    /// and try to maintain
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.desired_pool_size = v;
        self
    }

    /// Sets the behaviour of the pool on checkouts if no specific behaviour
    /// was requested by the user.
    pub fn default_checkout_mode<T: Into<DefaultPoolCheckoutMode>>(mut self, v: T) -> Self {
        self.default_checkout_mode = v.into();
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

    /// Sets the `ContentionLimit` of the pool.
    pub fn contention_limit(mut self, v: ContentionLimit) -> Self {
        self.contention_limit = v;
        self
    }

    /// When pool per node is created, sets a multiplier
    /// for the amount of pools per node to be created.
    ///
    /// Increasing this values reduces the contention on each created pool
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
    /// * `DEFAULT_POOL_CHECKOUT_MODE`: The default checkout mode to use. Omit if you do not want to update the value
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `NODE_POOL_STRATEGY`: Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
    /// * `CONTENTION_LIMIT`: Omit if you do not want to update the value
    pub fn update_from_environment(&mut self, prefix: Option<&str>) -> InitializationResult<()> {
        helpers::set_desired_pool_size(prefix, |v| {
            self.desired_pool_size = v;
        })?;

        helpers::set_default_checkout_mode(prefix, |v| {
            self.default_checkout_mode = v.adjust();
        })?;

        helpers::set_reservation_limit(prefix, |v| {
            self.reservation_limit = v;
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

        helpers::set_contention_limit(prefix, |v| {
            self.contention_limit = v;
        })?;

        Ok(())
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .default_checkout_mode(self.default_checkout_mode.adjust())
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
            .min_required_nodes(self.min_required_nodes)
            .connect_to_nodes(self.connect_to_nodes.clone())
            .node_pool_strategy(self.node_pool_strategy)
            .pool_per_node_multiplier(self.pool_per_node_multiplier)
            .contention_limit(self.contention_limit)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            default_checkout_mode: DefaultPoolCheckoutMode::WaitAtMost(Duration::from_millis(30)),
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: Some(100),
            activation_order: ActivationOrder::default(),
            min_required_nodes: 1,
            connect_to_nodes: Vec::new(),
            node_pool_strategy: NodePoolStrategy::default(),
            pool_per_node_multiplier: 1,
            contention_limit: ContentionLimit::NoLimit,
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
    /// when no idle connections are available
    pub fn reservation_limit(mut self, v: Option<usize>) -> Self {
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

    /// Sets the `NodePoolStrategy` to be used when creating the pool.
    pub fn node_pool_strategy(mut self, v: NodePoolStrategy) -> Self {
        self.config.node_pool_strategy = v;
        self
    }

    /// When pool per node is created, sets a multiplier
    /// for the amount of pools per node to be created.
    ///
    /// Increasing this values reduces the contention on each created pool
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: (`desired_pool_size`/multiplier) +1
    pub fn pool_per_node_multiplier(mut self, v: u32) -> Self {
        self.config.pool_per_node_multiplier = v;
        self
    }

    /// Sets the `ContentionLimit` of the pool.
    pub fn contention_limit(mut self, v: ContentionLimit) -> Self {
        self.config.contention_limit = v;
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
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `NODE_POOL_STRATEGY`: ` Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
    /// * `CONTENTION_LIMIT`: Omit if you do not want to update the value
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
    /// * `RESERVATION_LIMIT`: `usize` or `"NONE"`. Omit if you do not want to update the value
    /// * `ACTIVATION_ORDER`: `string`. Omit if you do not want to update the value
    /// * `MIN_REQUIRED_NODES`: `usize`. Omit if you do not want to update the value
    /// * `CONNECT_TO`: `[String]`. Separated by `;`. Omit if you do not want to update the value
    /// * `NODE_POOL_STRATEGY`: ` Omit if you do not want to update the value
    /// * `POOL_PER_NODE_MULTIPLIER`: Omit if you do not want to update the value
    /// * `CONTENTION_LIMIT`: Omit if you do not want to update the value
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

        if let Some(0) = config.contention_limit.limit() {
            return Err(InitializationError::message_only(
                "contention limit may not be ContentionLimit::Limited(0)",
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

/// Various options on retrieving a connection
/// that can be applied if a user wants to use the pool defaults
/// for retrieving a connection.
///
/// The default is to wait for 30ms.
///
/// This struct only slightly differs from `CheckoutMode`: It lacks
/// the variant `PoolDefault` since that variant would make no sense
/// as this enum describes the default behaviour of the pool.
///
/// This struct has the same behaviour as `CheckoutMode` regarding its
/// `From` implementations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DefaultPoolCheckoutMode {
    /// Expect a connection to be returned immediately.
    /// If there is none available return an error immediately.
    Immediately,
    /// Wait until there is a connection even if it would take forever.
    Wait,
    /// Wait for at most the given `Duration`.
    ///
    /// The amount of time waited will in the end not be really exact.
    WaitAtMost(Duration),
}

impl DefaultPoolCheckoutMode {
    /// Do a sanity adjustment. E.g. it makes no sense to use
    /// `WaitAtMost(Duration::from_desc(0))` since this would logically be
    /// `Immediately`.
    pub fn adjust(self) -> Self {
        match self {
            DefaultPoolCheckoutMode::WaitAtMost(d) if d == Duration::from_secs(0) => {
                DefaultPoolCheckoutMode::Immediately
            }
            x => x,
        }
    }
}

impl std::str::FromStr for DefaultPoolCheckoutMode {
    type Err = ParseDefaultPoolCheckoutModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "immediately" => Ok(DefaultPoolCheckoutMode::Immediately),
            "wait" => Ok(DefaultPoolCheckoutMode::Wait),
            milliseconds => Ok(DefaultPoolCheckoutMode::WaitAtMost(Duration::from_millis(
                milliseconds
                    .parse::<u64>()
                    .map_err(|err| ParseDefaultPoolCheckoutModeError(err.to_string()))?,
            ))),
        }
    }
}

impl From<Immediately> for DefaultPoolCheckoutMode {
    fn from(_: Immediately) -> Self {
        DefaultPoolCheckoutMode::Immediately
    }
}

impl From<Wait> for DefaultPoolCheckoutMode {
    fn from(_: Wait) -> Self {
        DefaultPoolCheckoutMode::Wait
    }
}

impl From<Duration> for DefaultPoolCheckoutMode {
    fn from(d: Duration) -> Self {
        if d != Duration::from_secs(0) {
            DefaultPoolCheckoutMode::WaitAtMost(d)
        } else {
            DefaultPoolCheckoutMode::Immediately
        }
    }
}

#[derive(Debug)]
pub struct ParseDefaultPoolCheckoutModeError(String);

impl fmt::Display for ParseDefaultPoolCheckoutModeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Could not parse ParseDefaultPoolCheckoutMode: {}",
            self.0
        )
    }
}

impl std::error::Error for ParseDefaultPoolCheckoutModeError {
    fn description(&self) -> &str {
        "parse default pool checkout mode failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

/// The maximum contention
#[derive(Debug, Copy, Clone)]
pub enum ContentionLimit {
    NoLimit,
    Limited(usize),
}

impl ContentionLimit {
    pub fn limit(self) -> Option<usize> {
        match self {
            ContentionLimit::Limited(n) => Some(n),
            ContentionLimit::NoLimit => None,
        }
    }
}

impl std::str::FromStr for ContentionLimit {
    type Err = ParseContentionLimitError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match &*s.to_lowercase() {
            "no-limit" => Ok(ContentionLimit::NoLimit),
            limit => {
                let limit = limit
                    .parse::<usize>()
                    .map_err(|err| ParseContentionLimitError(err.to_string()))?;
                Ok(ContentionLimit::Limited(limit))
            }
        }
    }
}

#[derive(Debug)]
pub struct ParseContentionLimitError(String);

impl fmt::Display for ParseContentionLimitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Could not parse ContentionLimit: {}", self.0)
    }
}

impl std::error::Error for ParseContentionLimitError {
    fn description(&self) -> &str {
        "parse contention limit failed"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        None
    }
}

fn create_no_pool(_instrumentation: InstrumentationFlavour) -> RedisPool {
    RedisPool(RedisPoolFlavour::Empty)
}
