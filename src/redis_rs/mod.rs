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

    use futures::{FutureExt, TryFutureExt};
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
                self.client
                    .get_async_connection()
                    .map_ok(|conn| ConnectionFlavour::RedisRs(conn, connected_to))
                    .map_err(NewConnectionError::new)
                    .boxed()
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

    use futures::{Future, FutureExt, TryFutureExt};
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

            let mut attempts_left = self.clients.len();
            let clients = Arc::clone(&self.clients);
            let connects_to = Arc::clone(&self.connects_to);

            async {
                loop {
                    if attempts_left == 0 {
                        return Err(NewConnectionError::new(
                            AllConnectAttemptsFailedError,
                        ));
                    }

                    let idx = (count + attempts_left) % clients.len();

                    let conn = get_async_connection(&clients[idx], Arc::clone(&connects_to[idx]));

                    match conn.await {
                        Ok(conn) => return Ok(conn),
                        Err(err) => {
                            warn!("failed to connect to {}: {}", connects_to[idx], err);
                            attempts_left -= 1;
                            continue;
                        }
                    }
                }
            }.boxed()
        }

        fn connecting_to(&self) -> Cow<[Arc<String>]> {
            Cow::Borrowed(&self.connects_to)
        }
    }

    fn get_async_connection(
        client: &Client,
        connects_to: Arc<String>,
    ) -> impl Future<Output = Result<ConnectionFlavour, NewConnectionError>> + '_ {
        client
            .get_async_connection()
            .map_ok(|conn| ConnectionFlavour::RedisRs(conn, connects_to))
            .map_err(NewConnectionError::new)
    }
}
