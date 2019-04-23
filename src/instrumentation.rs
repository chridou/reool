use std::time::Duration;

pub trait Instrumentation {
    fn checked_out_connection(&self);

    fn checked_in_returned_connection(&self, flight_time: Duration);

    fn checked_in_new_connection(&self);

    fn dropped_connection(&self, flight_time: Duration);

    fn idle_connections_changed(&self, len: usize);

    fn connection_created(&self, connected_after: Duration, total_time: Duration);

    fn killed_connection(&self, lifetime: Duration);

    fn reservations_changed(&self, len: usize);

    fn reservation_added(&self);

    fn reservation_fulfilled(&self, after: Duration);

    fn reservation_not_fulfilled(&self, after: Duration);

    fn reservation_limit_reached(&self);

    fn connection_factory_failed(&self);
}

impl Instrumentation for () {
    fn checked_out_connection(&self) {}
    fn checked_in_returned_connection(&self, _flight_time: Duration) {}
    fn checked_in_new_connection(&self) {}
    fn dropped_connection(&self, _flight_time: Duration) {}
    fn idle_connections_changed(&self, _len: usize) {}
    fn connection_created(&self, _connected_after: Duration, _total_time: Duration) {}
    fn killed_connection(&self, _lifetime: Duration) {}
    fn reservations_changed(&self, _len: usize) {}
    fn reservation_added(&self) {}
    fn reservation_fulfilled(&self, _after: Duration) {}
    fn reservation_not_fulfilled(&self, _after: Duration) {}
    fn reservation_limit_reached(&self) {}
    fn connection_factory_failed(&self) {}
}
