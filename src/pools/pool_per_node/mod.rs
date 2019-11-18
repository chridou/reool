//! A connection pool for connecting to the nodes of a replica set
use std::sync::Arc;
use std::time::Instant;

use futures::future::Future;
use log::info;

use crate::config::Config;
use crate::connection_factory::ConnectionFactory;
use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::InstrumentationFlavour;
use crate::pools::pool_internal::CheckoutManaged;
use crate::{Ping, PoolState, Poolable};

use super::{CanCheckout, CheckoutConstraint};

mod inner;

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
pub(crate) struct PoolPerNode<T: Poolable> {
    inner: Arc<(Inner<T>, Vec<String>)>,
}

impl<T: Poolable> PoolPerNode<T> {
    pub fn new<F, CF>(
        config: Config,
        create_connection_factory: F,
        executor_flavour: ExecutorFlavour,
        instrumentation: InstrumentationFlavour,
    ) -> InitializationResult<PoolPerNode<T>>
    where
        CF: ConnectionFactory<Connection = T> + Send + Sync + 'static,
        F: Fn(String) -> InitializationResult<CF>,
    {
        info!(
            "Creating pool per node for {:?} nodes",
            config.connect_to_nodes
        );

        let connected_to = config.connect_to_nodes.clone();

        let inner = Inner::new(
            config,
            create_connection_factory,
            executor_flavour,
            instrumentation,
        )?;

        Ok(PoolPerNode {
            inner: Arc::new((inner, connected_to)),
        })
    }

    pub fn connected_to(&self) -> &[String] {
        &(self.inner.1)
    }

    pub fn state(&self) -> PoolState {
        self.inner.0.state()
    }

    pub fn ping(&self, timeout: Instant) -> impl Future<Item = Vec<Ping>, Error = ()> + Send {
        self.inner.0.ping(timeout)
    }
}

impl<T: Poolable> CanCheckout<T> for PoolPerNode<T> {
    fn check_out<M: Into<CheckoutConstraint>>(&self, constraint: M) -> CheckoutManaged<T> {
        self.inner.0.check_out(constraint.into())
    }
}

impl<T: Poolable> Clone for PoolPerNode<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}
