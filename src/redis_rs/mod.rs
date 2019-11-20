use std::sync::Arc;
use std::time::Instant;

use futures::future::{self, Future};
use redis::Client;
use tokio::timer::Timeout;

use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
use crate::error::{InitializationError, InitializationResult};
use crate::pool_connection::ConnectionFlavour;
use crate::{Ping, PingState};

pub struct RedisRsFactory {
    client: Client,
    connects_to: Arc<String>,
}

impl RedisRsFactory {
    pub fn new(connect_to: String) -> InitializationResult<Self> {
        let client = Client::open(&*connect_to).map_err(|err| {
            InitializationError::new(
                format!("Could not create a redis-rs client to {}", connect_to),
                Some(Box::new(err)),
            )
        })?;

        Ok(Self {
            client,
            connects_to: (Arc::new(connect_to)),
        })
    }
}

impl ConnectionFactory for RedisRsFactory {
    type Connection = ConnectionFlavour;

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        let connected_to = Arc::clone(&self.connects_to);
        NewConnection::new(
            self.client
                .get_async_connection()
                .map(|conn| ConnectionFlavour::RedisRs(conn, connected_to))
                .map_err(NewConnectionError::new),
        )
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
