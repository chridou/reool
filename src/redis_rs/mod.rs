use std::borrow::Cow;
use std::sync::Arc;

use crate::connection_factory::{ConnectionFactory, NewConnection};
use crate::error::{InitializationError, InitializationResult};
use crate::pooled_connection::ConnectionFlavour;

pub enum RedisRsFactory {
    Single(single_node::RedisRsFactory),
    Multi(multi_node::RedisRsFactory),
}

impl RedisRsFactory {
    pub fn new(mut connect_to: Vec<String>) -> InitializationResult<RedisRsFactory> {
        if connect_to.is_empty() {
            return Err(InitializationError::message_only(
                "'connect_to' must not be empty",
            ));
        }

        if connect_to.len() == 1 {
            single_node::RedisRsFactory::new(connect_to.pop().unwrap()).map(RedisRsFactory::Single)
        } else {
            multi_node::RedisRsFactory::new(connect_to).map(RedisRsFactory::Multi)
        }
    }
}

impl ConnectionFactory for RedisRsFactory {
    type Connection = ConnectionFlavour;

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        match self {
            RedisRsFactory::Single(ref f) => f.create_connection(),
            RedisRsFactory::Multi(ref f) => f.create_connection(),
        }
    }

    fn connecting_to(&self) -> Cow<[Arc<String>]> {
        match self {
            RedisRsFactory::Single(ref f) => f.connecting_to(),
            RedisRsFactory::Multi(ref f) => f.connecting_to(),
        }
    }
}

mod single_node {
    use std::borrow::Cow;
    use std::sync::Arc;

    use futures::future::Future;
    use redis::Client;

    use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
    use crate::error::{InitializationError, InitializationResult};
    use crate::pooled_connection::ConnectionFlavour;

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

        fn connecting_to(&self) -> Cow<[Arc<String>]> {
            Cow::Owned(vec![Arc::clone(&self.connects_to)])
        }
    }
}

mod multi_node {
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use futures::future::{self, loop_fn, Future, Loop};
    use log::warn;
    use redis::Client;

    use crate::connection_factory::{ConnectionFactory, NewConnection, NewConnectionError};
    use crate::error::{InitializationError, InitializationResult};
    use crate::pooled_connection::ConnectionFlavour;

    pub struct RedisRsFactory {
        clients: Arc<Vec<Client>>,
        connects_to: Arc<Vec<Arc<String>>>,
        conn_counter: AtomicUsize,
    }

    impl RedisRsFactory {
        pub fn new(connect_to: Vec<String>) -> InitializationResult<Self> {
            let mut clients = Vec::new();
            let mut connects_to = Vec::new();
            for connect_to_uri in connect_to {
                let client = Client::open(&*connect_to_uri).map_err(|err| {
                    InitializationError::new(
                        format!("Could not create a redis-rs client to {}", connect_to_uri),
                        Some(Box::new(err)),
                    )
                })?;
                clients.push(client);
                connects_to.push(Arc::new(connect_to_uri));
            }
            Ok(Self {
                clients: Arc::new(clients),
                connects_to: Arc::new(connects_to),
                conn_counter: AtomicUsize::new(0),
            })
        }
    }

    #[derive(Debug)]
    struct AllConnectAttemptsFailedError;

    impl fmt::Display for AllConnectAttemptsFailedError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str(self.description())
        }
    }

    impl StdError for AllConnectAttemptsFailedError {
        fn description(&self) -> &str {
            "all nodes have been tried but no connection could be established"
        }

        fn cause(&self) -> Option<&dyn StdError> {
            None
        }
    }

    impl ConnectionFactory for RedisRsFactory {
        type Connection = ConnectionFlavour;

        fn create_connection(&self) -> NewConnection<Self::Connection> {
            let count = self.conn_counter.fetch_add(1, Ordering::SeqCst);

            let loop_f = loop_fn(
                (
                    self.clients.len(),
                    Arc::clone(&self.clients),
                    Arc::clone(&self.connects_to),
                ),
                move |(attempts_left, clients, connects_to)| {
                    if attempts_left == 0 {
                        return Box::new(future::err(NewConnectionError::new(
                            AllConnectAttemptsFailedError,
                        )))
                            as Box<dyn Future<Item = _, Error = _> + Send>;
                    }

                    let idx = (count + attempts_left) % clients.len();
                    let f = get_async_connection(&clients[idx], Arc::clone(&connects_to[idx]))
                        .then(move |r| match r {
                            Ok(conn) => Ok(Loop::Break(conn)),
                            Err(err) => {
                                warn!("failed to connect to {}: {}", connects_to[idx], err);
                                Ok(Loop::Continue((attempts_left - 1, clients, connects_to)))
                            }
                        });

                    Box::new(f)
                },
            );

            NewConnection::new(loop_f)
        }

        fn connecting_to(&self) -> Cow<[Arc<String>]> {
            Cow::Borrowed(&self.connects_to)
        }
    }

    fn get_async_connection(
        client: &Client,
        connects_to: Arc<String>,
    ) -> NewConnection<ConnectionFlavour> {
        NewConnection::new(
            client
                .get_async_connection()
                .map(|conn| ConnectionFlavour::RedisRs(conn, connects_to))
                .map_err(NewConnectionError::new),
        )
    }
}
