//! Pluggable instrumentation
use std::time::Duration;

/// A trait with methods that get called by the pool on certain events.
pub trait Instrumentation {
    /// A connection was checked out
    fn checked_out_connection(&self);

    /// A connection that was previously checked out was checked in again
    fn checked_in_returned_connection(&self, flight_time: Duration);

    /// A newly created connection was checked in
    fn checked_in_new_connection(&self);

    /// A connection was dropped because it was marked as defect
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration);

    /// The number of idle connections changed
    fn idle_connections_changed(&self, len: usize);

    /// A new connection was created
    fn connection_created(&self, connected_after: Duration, total_time: Duration);

    /// A connection was intentionally killed. Happens when connections are removed.
    fn killed_connection(&self, lifetime: Duration);

    /// The number of reservations in the reservation queue changed
    fn reservations_changed(&self, len: usize);

    /// A reservation has been enqueued
    fn reservation_added(&self);

    /// A reservation was fulfilled. A connection was available in time.
    fn reservation_fulfilled(&self, after: Duration);

    /// A reservation was not fulfilled. A connection was mostly not available in time.
    fn reservation_not_fulfilled(&self, after: Duration);

    /// The reservation queue has a limit and that limit was just reached.
    /// This means a checkout has instantaneously failed.
    fn reservation_limit_reached(&self);

    /// The connection factory was asked to create a new connection but it failed to do so.
    fn connection_factory_failed(&self);
}

impl Instrumentation for () {
    fn checked_out_connection(&self) {}
    fn checked_in_returned_connection(&self, _flight_time: Duration) {}
    fn checked_in_new_connection(&self) {}
    fn connection_dropped(&self, _flight_time: Duration, _lifetime: Duration) {}
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

#[cfg(feature = "metrix")]
pub(crate) mod metrix {
    use std::time::Duration;

    use metrix::cockpit::Cockpit;
    use metrix::instruments::*;
    use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
    use metrix::{TelemetryTransmitter, TransmitsTelemetryData};

    use super::Instrumentation;

    #[derive(Clone, Copy, Eq, PartialEq)]
    enum Metric {
        CheckOutConnection,
        CheckedInReturnedConnection,
        CheckedInNewConnection,
        ConnectionDropped,
        ConnectionKilled,
        IdleConnectionsUpdated,
        ConnectionCreated,
        ConnectionCreatedTotalTime,
        ReservationsUpdated,
        ReservationAdded,
        ReservationFulfilled,
        ReservationNotFulfilled,
        ReservationLimitReached,
        ConnectionFactoryFailed,
        LifeTime,
    }

    pub fn create<A: AggregatesProcessors>(aggregates_processors: &mut A) -> MetrixInstrumentation {
        let mut cockpit = Cockpit::without_name(None);

        let mut panel = Panel::with_name(Metric::CheckOutConnection, "checked_out_connections");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::CheckedInReturnedConnection,
            "checked_in_returned_connections",
        );
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("flight_time_us"));
        cockpit.add_panel(panel);

        let mut panel =
            Panel::with_name(Metric::CheckedInNewConnection, "checked_in_new_connections");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionDropped, "connections_dropped");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("flight_time_us"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionKilled, "connections_killed");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::IdleConnectionsUpdated, "idle_connections");
        panel.set_gauge(Gauge::new_with_defaults("count"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionCreated, "connections_created");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("connect_time_us"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::ConnectionCreatedTotalTime,
            "connections_created_total",
        );
        panel.set_value_scaling(ValueScaling::NanosToMillis);
        panel.set_histogram(Histogram::new_with_defaults("time_ms"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsUpdated, "reservations");
        panel.set_gauge(Gauge::new_with_defaults("count"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationAdded, "reservations_added");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationFulfilled, "reservations_fulfilled");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("fulfilled_after_us"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::ReservationNotFulfilled,
            "reservations_not_fulfilled",
        );
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        panel.set_histogram(Histogram::new_with_defaults("not_fulfilled_after_us"));
        cockpit.add_panel(panel);

        let mut panel =
            Panel::with_name(Metric::ReservationLimitReached, "reservation_limit_reached");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel =
            Panel::with_name(Metric::ConnectionFactoryFailed, "connection_factory_failed");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::LifeTime, "life_times");
        panel.set_value_scaling(ValueScaling::NanosToMillis);
        panel.set_meter(Meter::new_with_defaults("lifes_ended_per_second"));
        panel.set_histogram(Histogram::new_with_defaults("life_time_ms"));
        cockpit.add_panel(panel);

        let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();
        rx.add_cockpit(cockpit);

        aggregates_processors.add_processor(rx);

        MetrixInstrumentation { transmitter: tx }
    }

    #[derive(Clone)]
    pub struct MetrixInstrumentation {
        transmitter: TelemetryTransmitter<Metric>,
    }

    impl Instrumentation for MetrixInstrumentation {
        fn checked_out_connection(&self) {
            self.transmitter
                .observed_one_now(Metric::CheckOutConnection);
        }
        fn checked_in_returned_connection(&self, flight_time: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::CheckedInReturnedConnection, flight_time);
        }
        fn checked_in_new_connection(&self) {
            self.transmitter
                .observed_one_now(Metric::CheckedInNewConnection);
        }
        fn connection_dropped(&self, flight_time: Duration, lifetime: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::ConnectionDropped, flight_time)
                .observed_one_duration_now(Metric::LifeTime, lifetime);
        }
        fn idle_connections_changed(&self, len: usize) {
            self.transmitter
                .observed_one_value_now(Metric::IdleConnectionsUpdated, len as u64);
        }
        fn connection_created(&self, connected_after: Duration, total_time: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::ConnectionCreated, connected_after)
                .observed_one_duration_now(Metric::ConnectionCreatedTotalTime, total_time);
        }
        fn killed_connection(&self, lifetime: Duration) {
            self.transmitter
                .observed_one_now(Metric::ConnectionKilled)
                .observed_one_duration_now(Metric::LifeTime, lifetime);
        }
        fn reservations_changed(&self, len: usize) {
            self.transmitter
                .observed_one_value_now(Metric::ReservationsUpdated, len as u64);
        }
        fn reservation_added(&self) {
            self.transmitter.observed_one_now(Metric::ReservationAdded);
        }
        fn reservation_fulfilled(&self, after: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::ReservationFulfilled, after);
        }
        fn reservation_not_fulfilled(&self, after: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::ReservationNotFulfilled, after);
        }
        fn reservation_limit_reached(&self) {
            self.transmitter
                .observed_one_now(Metric::ReservationLimitReached);
        }
        fn connection_factory_failed(&self) {
            self.transmitter
                .observed_one_now(Metric::ConnectionFactoryFailed);
        }
    }
}
