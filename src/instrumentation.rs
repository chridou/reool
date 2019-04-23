use std::time::Duration;

pub trait Instrumentation {
    fn notify_checked_out(&self);

    fn notify_checked_in_returned_connection(&self, flight_time: Duration);

    fn notify_checked_in_new_connection(&self);

    fn notify_connection_dropped(&self, flight_time: Duration);

    fn notify_idle_connections_changed(&self, v: usize);

    fn notify_created(&self, connected_after: Duration, total_time: Duration);

    fn notify_connection_killed(&self, lifetime: Duration);

    fn notify_waiting_queue_length(&self, len: usize);

    fn notify_waiting(&self);

    fn notify_fulfilled(&self, after: Duration);

    fn notify_not_fulfilled(&self, after: Duration);

    fn notify_connection_factory_failed(&self);

    fn notify_queue_limit_reached(&self);
}
