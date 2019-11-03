//! A connection pool for connecting to the nodes of a replica set
use std::sync::Arc;
use std::time::Duration;

use futures::future::Future;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::Instrumentation;
use crate::pooled_connection::ConnectionFlavour;
use crate::stats::PoolStats;
use crate::{Checkout, Ping};

use super::pool_internal::PoolInternal;

mod inner;
mod instrumentation;

use self::inner::*;

/// A connection pool that maintains multiple connection pools
/// to a multiple Redis nodes whereas each inner pool is
/// connected to one Redis node
///
/// All the instances should be part of the same replica set.
/// You should only perform read operations on the
/// connections received from this kind of pool.
///
/// The replicas are selected in a round robin fashion.
///
/// The pool is cloneable and all clones share their connections.
/// Once the last instance drops the shared connections will be dropped.
pub struct PoolPerNode {
    inner: Arc<Inner>,
    checkout_timeout: Option<Duration>,
}

impl PoolPerNode {
    pub fn new<I, F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: Option<I>,
    ) -> InitializationResult<PoolPerNode>
    where
        I: Instrumentation + Send + Sync + 'static,
        CF: ConnectionFactory<Connection = ConnectionFlavour> + Send + Sync + 'static,
        F: Fn(Vec<String>) -> InitializationResult<CF>,
    {
        let checkout_timeout = config.checkout_timeout;
        let inner = Inner::new(
            config,
            create_connection_factory,
            executor_flavour,
            instrumentation,
        )?;

        Ok(PoolPerNode {
            inner: Arc::new(inner),
            checkout_timeout,
        })
    }

    pub fn check_out(&self) -> Checkout {
        self.inner.check_out_explicit_timeout(self.checkout_timeout)
    }

    pub fn check_out_explicit_timeout(&self, timeout: Option<Duration>) -> Checkout {
        self.inner.check_out_explicit_timeout(timeout)
    }

    /// Get some statistics from each of the pools.
    ///
    /// This locks the underlying pool.
    pub fn stats(&self) -> Vec<PoolStats> {
        self.inner.pools.iter().map(PoolInternal::stats).collect()
    }

    /// Triggers the pool to emit statistics if `stats_interval` has elapsed.
    ///
    /// This locks the underlying pool.
    pub fn trigger_stats(&self) {
        self.inner
            .pools
            .iter()
            .for_each(PoolInternal::trigger_stats)
    }

    pub fn ping(&self, timeout: Duration) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        self.inner.ping(timeout)
    }

    pub fn connected_to(&self) -> Vec<String> {
        self.inner
            .pools
            .iter()
            .flat_map(|p| p.connected_to())
            .map(ToOwned::to_owned)
            .collect()
    }
}

impl Clone for PoolPerNode {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            checkout_timeout: self.checkout_timeout,
        }
    }
}
