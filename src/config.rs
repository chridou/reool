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
//! Make sure not to write to that pool.
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use log::{debug, info, warn};

use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::helpers;
use crate::instrumentation::{Instrumentation, InstrumentationFlavour};
use crate::pools::{PoolPerNode, SinglePool};
use crate::redis_rs::RedisRsFactory;

pub use crate::activation_order::ActivationOrder;
pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::error::InitializationError;
use crate::{Immediately, Poolable, RedisPool, RedisPoolFlavour, Wait};

/// A configuration for creating a `RedisPool`.
#[derive(Debug, Clone)]
pub struct Config {
    /// The number of connections a pool should have. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
    pub desired_pool_size: usize,
    /// The timeout for a checkout if no specific timeout is given
    /// with a checkout.
    pub default_checkout_mode: DefaultPoolCheckoutMode,
    /// The `BackoffStrategy` to use when retrying on
    /// failures to create new connections
    pub backoff_strategy: BackoffStrategy,
    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
    pub reservation_limit: usize,
    /// Defines the `ActivationOrder` in which idle connections are
    /// activated.
    ///
    /// Default is `ActivationOrder::FiFo`
    pub activation_order: ActivationOrder,
    /// The minimum required nodes to start
    pub min_required_nodes: usize,
    /// The nodes to connect to.
    pub connect_to_nodes: Vec<String>,
    /// When the pool is created this is a multiplier for the amount of sub
    /// pools to be created.
    ///
    /// Other values will be adjusted if the multiplier is > 1:
    ///
    /// * `reservation_limit`: Stays zero if zero, otherwise (`reservation_limit`/multiplier) +1
    /// * `desired_pool_size`: `desired_pool_size`/multiplier) +1
    pub pool_multiplier: u32,
    /// The number of checkouts that can be enqueued. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 100.
    pub checkout_queue_size: usize,
}

impl Config {
    /// The number of connections a pool should have. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
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

    /// The maximum length of the queue for waiting checkouts
    /// when no idle connections are available. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 50.
    pub fn reservation_limit(mut self, v: usize) -> Self {
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

    /// The number of checkouts that can be enqueued. If a pool with
    /// multiple sub pools is created, this value applies to each
    /// sub pool.
    ///
    /// The default is 100.
    pub fn checkout_queue_size(mut self, v: usize) -> Self {
        self.checkout_queue_size = v;
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
        self.pool_multiplier = v;
        self
    }

    /// Updates this configuration from the environment.
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

        helpers::set_pool_multiplier(prefix, |v| {
            self.pool_multiplier = v;
        })?;

        helpers::set_checkout_queue_size(prefix, |v| {
            self.checkout_queue_size = v;
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
            .pool_multiplier(self.pool_multiplier)
            .checkout_queue_size(self.checkout_queue_size)
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 50,
            default_checkout_mode: DefaultPoolCheckoutMode::WaitAtMost(Duration::from_millis(30)),
            backoff_strategy: BackoffStrategy::default(),
            reservation_limit: 50,
            activation_order: ActivationOrder::default(),
            min_required_nodes: 1,
            connect_to_nodes: Vec::new(),
            pool_multiplier: 1,
            checkout_queue_size: 100,
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

        Ok(RedisPool(flavour))
    }

    /// Build a new `RedisPool`
    pub fn finish_redis_rs(self) -> InitializationResult<RedisPool> {
        self.finish(RedisRsFactory::new)
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
    /// Wait until there is a connection.
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
            "wait" => Ok(DefaultPoolCheckoutMode::Wait),
            "immediately" => Ok(DefaultPoolCheckoutMode::Immediately),
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

fn create_no_pool<T: Poolable>(_instrumentation: InstrumentationFlavour) -> RedisPool<T> {
    RedisPool(RedisPoolFlavour::Empty)
}
