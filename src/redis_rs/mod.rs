use std::sync::Arc;
use std::time::Instant;

use future::BoxFuture;
use futures::prelude::*;
use redis::IntoConnectionInfo;
use tokio::time::timeout_at;
use trust_dns_resolver::TokioAsyncResolver;

use crate::connection_factory::ConnectionFactory;
use crate::error::{Error, InitializationResult};
use crate::pool_connection::ConnectionFlavour;
use crate::{Ping, PingState};

pub struct RedisRsFactory {
    connects_to: Arc<String>,
}

impl RedisRsFactory {
    pub fn new(connect_to: String) -> InitializationResult<Self> {
        Ok(Self {
            connects_to: (Arc::new(connect_to)),
        })
    }
}

impl ConnectionFactory for RedisRsFactory {
    type Connection = ConnectionFlavour;

    fn create_connection(&self) -> BoxFuture<Result<Self::Connection, Error>> {
        async move {
            // FIXME: Doesn't work with URLs without host (e.g. unix sockets)
            // This should ideally be implemented in the redis crate.

            let mut url = redis::parse_redis_url(&self.connects_to)
                .map_err(|()| Error::message(format!("Invalid redis url: {}", self.connects_to)))?;

            let resolver = TokioAsyncResolver::tokio_from_system_conf()
                .await
                .map_err(|err| Error::new("Failed to resolve", Some(err)))?;

            let host = url
                .host_str()
                .ok_or_else(|| Error::message("Redis url has no host part"))?;

            let addrs = resolver
                .lookup_ip(host)
                .await
                .map_err(|err| Error::new("Failed to look up address", Some(err)))?;

            let addr = addrs
                .into_iter()
                .next()
                .ok_or_else(|| Error::message("No addresses were returned"))?;

            url.set_ip_host(addr).ok();

            let connection_info = url.into_connection_info().map_err(|err| {
                Error::new("Failed to turn redis url into connection inf", Some(err))
            })?;
            let connection = redis::aio::connect_tokio(&connection_info).await?;
            let connection = ConnectionFlavour::RedisRs(connection, self.connects_to.clone());

            Ok(connection)
        }
        .boxed()
    }

    fn connecting_to(&self) -> &str {
        &*self.connects_to
    }

    fn ping(&self, timeout: Instant) -> BoxFuture<Ping> {
        use crate::redis_ops::RedisOps;

        let started_at = Instant::now();

        let ping_future = async move {
            let connection = self.create_connection().await;
            let connect_time = Some(started_at.elapsed());
            let uri = self.connecting_to().to_owned();

            let mut connection = match connection {
                Ok(connection) => connection,
                Err(err) => {
                    return Ping {
                        uri,
                        state: PingState::failed_msg(format!(
                            "failed to create connection: {}",
                            err
                        )),
                        connect_time,
                        latency: None,
                        total_time: started_at.elapsed(),
                    }
                }
            };

            let ping_started_at = Instant::now();
            let ping_result = connection.ping().await;
            let latency = Some(ping_started_at.elapsed());

            let state = match ping_result {
                Ok(_) => PingState::Ok,
                Err(err) => PingState::failed_msg(format!("ping failed: {}", err)),
            };

            Ping {
                uri: self.connecting_to().to_owned(),
                state,
                connect_time,
                latency,
                total_time: started_at.elapsed(),
            }
        };

        timeout_at(timeout.into(), ping_future)
            .unwrap_or_else(move |_| {
                let total_time = started_at.elapsed();
                let uri = self.connecting_to().to_owned();
                let state = PingState::failed_msg(format!(
                    "ping to '{}' timed out after {:?}",
                    uri, total_time
                ));

                Ping {
                    uri,
                    latency: None,
                    connect_time: None,
                    total_time,
                    state,
                }
            })
            .boxed()
    }
}
