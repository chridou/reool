use std::sync::Arc;
use std::time::Duration;

use futures::future::Future;
use redis::r#async::Connection;

use crate::backoff_strategy::BackoffStrategy;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::pool::{Config as PoolConfig, Pool};
use crate::*;

pub struct Config {
    pub desired_pool_size: usize,
    pub backoff_strategy: BackoffStrategy,
    pub checkout_timeout: Option<Duration>,
    pub wait_queue_limit: Option<usize>,
}

impl Config {
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.desired_pool_size = v;
        self
    }

    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.checkout_timeout = v;
        self
    }

    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.backoff_strategy = v;
        self
    }

    pub fn wait_queue_limit(mut self, v: Option<usize>) -> Self {
        self.wait_queue_limit = v;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            desired_pool_size: 20,
            checkout_timeout: Some(Duration::from_millis(20)),
            backoff_strategy: BackoffStrategy::default(),
            wait_queue_limit: Some(50),
        }
    }
}

pub struct SingleNodePool {
    pool: Arc<Pool<Connection>>,
    checkout_timeout: Option<Duration>,
}

impl SingleNodePool {
    pub(crate) fn create<T: IntoConnectionInfo>(
        config: Config,
        connect_to: T,
        executor_flavour: ExecutorFlavour,
    ) -> InitializationResult<Self> {
        let client = Client::open(connect_to).map_err(|err| InitializationError::new(err))?;

        let pool_conf = PoolConfig {
            desired_pool_size: config.desired_pool_size,
            backoff_strategy: config.backoff_strategy,
            wait_queue_limit: config.wait_queue_limit,
        };

        let pool = Pool::new(pool_conf, client, executor_flavour);

        Ok(Self {
            pool: Arc::new(pool),
            checkout_timeout: config.checkout_timeout,
        })
    }
}

impl RedisPool for SingleNodePool {
    fn checkout(&self) -> Checkout {
        if let Some(timeout) = self.checkout_timeout {
            Checkout(self.pool.checkout(Some(timeout)))
        } else {
            Checkout(self.pool.checkout(None))
        }
    }

    fn checkout_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        Checkout(self.pool.checkout(timeout))
    }
}
