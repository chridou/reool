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
use std::time::Duration;

use crate::error::InitializationResult;
use crate::helpers;
use crate::Poolable;

pub use crate::activation_order::ActivationOrder;
pub use crate::backoff_strategy::BackoffStrategy;
pub use crate::error::InitializationError;
pub use builder::Builder;
pub use config_types::*;

mod builder;
mod config_types;

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
    /// Set to `true` if a retry on a checkout should be made if the queue was full.
    /// Otherwise do not retry.
    ///
    /// The default is `true`.
    pub retry_on_checkout_limit: bool,
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

    /// Set to `true` if a retry on a checkout should be made if the queue was full.
    /// Otherwise do not retry.
    ///
    /// The default is `true`.
    pub fn retry_on_checkout_limit(mut self, v: bool) -> Self {
        self.retry_on_checkout_limit = v;
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
    /// * `RETRY_ON_CHECKOUT_LIMIT`: Omit if you do not want to update the value
    pub fn update_from_environment(&mut self, prefix: Option<&str>) -> InitializationResult<()> {
        helpers::set_desired_pool_size(prefix, |v| {
            self.desired_pool_size = v;
        })?;

        helpers::set_default_checkout_mode(prefix, |v| {
            self.default_checkout_mode = v;
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

        helpers::set_retry_on_checkout_limit(prefix, |v| {
            self.retry_on_checkout_limit = v;
        })?;

        Ok(())
    }

    /// Create a `Builder` initialized with the values from this `Config`
    pub fn builder(&self) -> Builder {
        Builder::default()
            .desired_pool_size(self.desired_pool_size)
            .default_checkout_mode(self.default_checkout_mode)
            .backoff_strategy(self.backoff_strategy)
            .reservation_limit(self.reservation_limit)
            .min_required_nodes(self.min_required_nodes)
            .connect_to_nodes(self.connect_to_nodes.clone())
            .pool_multiplier(self.pool_multiplier)
            .checkout_queue_size(self.checkout_queue_size)
            .retry_on_checkout_limit(self.retry_on_checkout_limit)
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
            retry_on_checkout_limit: true,
        }
    }
}
