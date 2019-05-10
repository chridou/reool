

impl Poolable for Connection {}

impl ConnectionFactory for Client {
    type Connection = Connection;

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        NewConnection::new(self.get_async_connection().map_err(NewConnectionError::new))
    }
}
