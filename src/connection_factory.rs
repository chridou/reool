//! Building blocks for creating a `ConnectionFactory`
use std::time::{Duration, Instant};

use future::{self, BoxFuture};
use futures::prelude::*;

use crate::{error::Error, Ping, PingState, Poolable};

/// A factory for connections that always creates
/// connections to the same node.
///
/// The factory also performs pings on a node by using a fresh connection on each
/// ping.
pub trait ConnectionFactory {
    type Connection: Poolable;
    /// Create a new connection
    fn create_connection(&self) -> BoxFuture<Result<Self::Connection, Error>>;
    /// The node this factory will connect to when a new
    /// connection is requested.
    fn connecting_to(&self) -> &str;
    /// Ping the Redis node
    ///
    /// This will create a new connection and try a ping on it.
    ///
    /// If a factory does not support `ping` it will simply fail with `()`.
    fn ping(&self, _timeout: Instant) -> BoxFuture<Ping> {
        future::ready(Ping {
            connect_time: None,
            latency: None,
            total_time: Duration::default(),
            uri: self.connecting_to().to_owned(),
            state: PingState::failed_msg("pinging is not supported by this connection factory"),
        })
        .boxed()
    }
}
