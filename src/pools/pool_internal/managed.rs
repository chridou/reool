use std::time::Instant;

use log::{debug, trace};

use crate::Poolable;

use super::inner_pool::PoolMessage;

use super::extended_connection_factory::ExtendedConnectionFactory;

/// Contains a connection. This is the essential part of `Reool`.
///
/// We wrap the connection along with some other data in `Managed<T>`.
///
/// `Managed` tries to return to its pool on drop or creates a new
/// one if broken(`value`=`None`)
///
/// If `factory` is `None` the lifecycle of the connection ends and no new one
/// may be created.
pub(crate) struct Managed<T: Poolable> {
    pub created_at: Instant,
    /// Is `Some` taken from the pool otherwise fresh connection
    pub checked_out_at: Option<Instant>,
    /// The actual connection. If `None` this
    /// `Managed` may not return to the pool and
    /// a new connection shall be created
    pub value: Option<T>,
    /// If `None` the pool is gone and the life cycle definitely ends.
    /// No attempt to create a new connection may be made.
    pub factory: Option<ExtendedConnectionFactory<T>>,
}

impl<T: Poolable> Managed<T> {
    pub fn fresh(value: T, factory: ExtendedConnectionFactory<T>) -> Self {
        Managed {
            value: Some(value),
            created_at: Instant::now(),
            checked_out_at: None,
            factory: Some(factory),
        }
    }

    pub fn connected_to(&self) -> &str {
        self.value
            .as_ref()
            .expect("no value in managed - this is a bug")
            .connected_to()
    }

    /// This must be called before finally dropping a connection
    /// to prevent an infinite loop when dropping
    pub fn drop_orphanized(mut self) {
        self.factory = None; // the marker for being orphanized
        self.value = None; // just to make it complete. The actual connection can be closed
        drop(self); // be explicit on this!
    }
}

impl<T: Poolable> Drop for Managed<T> {
    fn drop(&mut self) {
        if self.factory.is_none() {
            trace!("Orphan goes into the void");
            return;
        }

        let factory = self.factory.take().unwrap();
        let mut send_back = factory.send_back_cloned();
        if let Some(value) = self.value.take() {
            let msg = PoolMessage::CheckIn {
                created_at: Instant::now(),
                conn: Managed {
                    value: Some(value),
                    created_at: self.created_at,
                    checked_out_at: self.checked_out_at,
                    factory: Some(factory), // Keeps it active
                },
            };
            if let Err(msg) = msg.send_on_internal_channel(&mut send_back) {
                debug!("inner pool gone - simply dropping");
                // We must "orphanize" the connection to avoid a drop loop
                drop_connection_orphanized(msg);
            } else {
                debug!("sent connection to pool");
            }
        } else {
            factory.instrumentation.connection_dropped(
                self.checked_out_at.map(|d| d.elapsed()),
                self.created_at.elapsed(),
            );
            debug!("no value - drop connection and request new one");
            // factory and value are already `None` so we can safely continue
            // dropping here
            factory.create_connection(Instant::now());
        }
    }
}

/// Drops the connection stored in a message. Even though in the context of `Managed`
/// there can only be one type of message we try all of them to prevent future errors.
///
/// Not dropping a connection properly would result in an infinite drop recursion
fn drop_connection_orphanized<T: Poolable>(msg: PoolMessage<T>) {
    // Always check all variants here!
    let conn = match msg {
        PoolMessage::CheckIn { conn, .. } => conn,
        PoolMessage::CheckOut { .. } => return,
        PoolMessage::CleanupReservations(_) => return,
        PoolMessage::CheckAlive(_) => return,
    };

    conn.drop_orphanized()
}
