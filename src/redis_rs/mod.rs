use std::error::Error;
use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future, IntoFuture};
use redis::IntoConnectionInfo;
use tokio::timer::Timeout;

use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
use crate::error::InitializationResult;
use crate::executor_flavour::ExecutorFlavour;
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

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        let connects_to1 = self.connects_to.clone();
        let connects_to2 = self.connects_to.clone();

        // FIXME: Doesn't work with URLs without host (e.g. unix sockets)
        // This should ideally be implemented in the redis crate.
        let connection_future = future::lazy(move || -> Result<_, Box<dyn Error + Send + Sync>> {
            let mut url = redis::parse_redis_url(&connects_to1)
                .map_err(|_| format!("Invalid redis url: {}", connects_to1))?;

            let (resolver, background_task) = trust_dns_resolver::AsyncResolver::from_system_conf()
                .map_err(|err| format!("Cannot create resolver: {}", err))?;

            ExecutorFlavour::Runtime
                .spawn(background_task)
                .map_err(|err| format!("Failed to spawn resolver background task: {}", err))?;

            let host = url.host_str().ok_or_else(|| "Redis url has no host part")?;

            let url_future = resolver
                .lookup_ip(host)
                .map_err(|err| format!("Failed to look up address: {}", err))
                .map_err(Box::<dyn Error + Send + Sync>::from)
                .and_then(|addrs| {
                    addrs
                        .into_iter()
                        .next()
                        .ok_or_else(|| "No addresses were returned")
                        .into_future()
                        .from_err()
                })
                .map(move |addr| {
                    url.set_ip_host(addr).ok();
                    url
                });

            Ok(url_future)
        })
        .flatten()
        .and_then(|url| {
            url.into_connection_info()
                .map_err(|err| format!("Failed to turn redis url into connection info: {}", err))
                .into_future()
                .from_err()
        })
        .and_then(|connection_info| redis::aio::connect(connection_info).from_err())
        .map(|connection| ConnectionFlavour::RedisRs(connection, connects_to2))
        .map_err(NewConnectionError::from);

        NewConnection::new(connection_future)
    }

    fn connecting_to(&self) -> &str {
        &*self.connects_to
    }

    fn ping(&self, timeout: Instant) -> Box<dyn Future<Item = Ping, Error = ()> + Send> {
        use crate::commands::Commands;

        let started_at = Instant::now();

        let uri = self.connecting_to().to_string();

        let f = self
            .create_connection()
            .then({
                let uri = uri.clone();
                move |conn| {
                    let connect_time = Some(started_at.elapsed());
                    match conn {
                        Ok(conn) => {
                            let ping_started_at = Instant::now();
                            let f = conn
                                .ping()
                                .then(move |r| {
                                    let latency = Some(ping_started_at.elapsed());
                                    let state = if let Err(err) = r {
                                        PingState::failed_msg(format!("ping failed: {}", err))
                                    } else {
                                        PingState::Ok
                                    };

                                    Ok(Ping {
                                        uri,
                                        state,
                                        connect_time,
                                        latency,
                                        total_time: started_at.elapsed(),
                                    })
                                })
                                .map_err(|_: ()| ());
                            Box::new(f) as Box<dyn Future<Item = _, Error = _> + Send>
                        }
                        Err(err) => Box::new(future::ok(Ping {
                            uri,
                            state: PingState::failed_msg(format!(
                                "failed to create connection: {}",
                                err
                            )),
                            connect_time,
                            latency: None,
                            total_time: started_at.elapsed(),
                        })),
                    }
                }
            })
            .map_err(|_| ());

        let f = Timeout::new_at(f, timeout).or_else(move |_| {
            let total_time = started_at.elapsed();
            let state = PingState::failed_msg(format!(
                "ping to '{}' timed out after {:?}",
                uri, total_time
            ));
            Ok(Ping {
                uri,
                latency: None,
                connect_time: None,
                total_time,
                state,
            })
        });

        Box::new(f)
    }
}
