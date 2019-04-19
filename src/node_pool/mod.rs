use std::sync::Arc;
use std::time::Duration;

use redis::r#async::Connection;

use crate::backoff_strategy::BackoffStrategy;
use crate::error::{InitializationError, InitializationResult};
use crate::executor_flavour::ExecutorFlavour;
use crate::pool::{Config as PoolConfig, Pool, PoolStats};
use crate::*;

pub struct Builder<T> {
    config: Config,
    executor_flavour: ExecutorFlavour,
    connect_to: T,
}

impl Default for Builder<()> {
    fn default() -> Self {
        Self {
            config: Config::default(),
            executor_flavour: ExecutorFlavour::Runtime,
            connect_to: (),
        }
    }
}

impl<T> Builder<T> {
    pub fn desired_pool_size(mut self, v: usize) -> Self {
        self.config.desired_pool_size = v;
        self
    }

    pub fn checkout_timeout(mut self, v: Option<Duration>) -> Self {
        self.config.checkout_timeout = v;
        self
    }

    pub fn backoff_strategy(mut self, v: BackoffStrategy) -> Self {
        self.config.backoff_strategy = v;
        self
    }

    pub fn wait_queue_limit(mut self, v: Option<usize>) -> Self {
        self.config.wait_queue_limit = v;
        self
    }

    pub fn connect_to<C: IntoConnectionInfo>(self, connect_to: C) -> Builder<C> {
        Builder {
            config: self.config,
            executor_flavour: self.executor_flavour,
            connect_to: connect_to,
        }
    }

    pub fn task_executor(mut self, executor: ::tokio::runtime::TaskExecutor) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioTaskExecutor(executor);
        self
    }

    pub fn handle(mut self, executor: ::tokio::runtime::current_thread::Handle) -> Self {
        self.executor_flavour = ExecutorFlavour::TokioHandle(executor);
        self
    }
}

impl<T> Builder<T>
where
    T: IntoConnectionInfo,
{
    pub fn finish(self) -> InitializationResult<SingleNodePool> {
        SingleNodePool::create(self.config, self.connect_to, self.executor_flavour)
    }
}

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
            wait_queue_limit: Some(100),
        }
    }
}

#[derive(Clone)]
pub struct SingleNodePool {
    pool: Arc<Pool<Connection>>,
    checkout_timeout: Option<Duration>,
}

impl SingleNodePool {
    pub fn builder() -> Builder<()> {
        Builder::default()
    }

    pub fn new<T: IntoConnectionInfo>(config: Config, connect_to: T) -> InitializationResult<Self> {
        Self::create(config, connect_to, ExecutorFlavour::Runtime)
    }

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

    pub fn stats(&self) -> PoolStats {
        self.pool.stats()
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
