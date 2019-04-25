//! Pluggable instrumentation
use std::time::Duration;

#[cfg(feature = "metrix")]
pub use self::metrix::MetrixConfig;

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
    ///
    /// * If there is only a single node `min` and `max` will be the same
    /// * If you have multiple nodes (e.g. replica set) it will be the min and max of all nodes
    fn idle_connections_changed(&self, min: usize, max: usize);

    /// A new connection was created
    fn connection_created(&self, connected_after: Duration, total_time: Duration);

    /// A connection was intentionally killed. Happens when connections are removed.
    fn killed_connection(&self, lifetime: Duration);

    /// The number of reservations in the reservation queue changed.
    ///
    /// * If there is only a single node `min` and `max` will be the same
    /// * If you have multiple nodes (e.g. replica set) it will be the min and max of all nodes
    ///
    /// Limit is the maximum length of the reservation queue
    fn reservations_changed(&self, min: usize, max: usize, limit: Option<usize>);

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

    /// The number of connections in the pool that can be used
    ///
    /// * If there is only a single node `min` and `max` will be the same
    /// * If you have multiple nodes (e.g. replica set) it will be the min and max of all nodes
    fn usable_connections_changed(&self, min: usize, max: usize);

    /// The number of connections in flight
    ///
    /// * If there is only a single node `min` and `max` will be the same
    /// * If you have multiple nodes (e.g. replica set) it will be the min and max of all nodes
    fn in_flight_connections_changed(&self, min: usize, max: usize);
}

impl Instrumentation for () {
    fn checked_out_connection(&self) {}
    fn checked_in_returned_connection(&self, _flight_time: Duration) {}
    fn checked_in_new_connection(&self) {}
    fn connection_dropped(&self, _flight_time: Duration, _lifetime: Duration) {}
    fn idle_connections_changed(&self, _min: usize, _max: usize) {}
    fn connection_created(&self, _connected_after: Duration, _total_time: Duration) {}
    fn killed_connection(&self, _lifetime: Duration) {}
    fn reservations_changed(&self, _min: usize, _max: usize, _limit: Option<usize>) {}
    fn reservation_added(&self) {}
    fn reservation_fulfilled(&self, _after: Duration) {}
    fn reservation_not_fulfilled(&self, _after: Duration) {}
    fn reservation_limit_reached(&self) {}
    fn connection_factory_failed(&self) {}
    fn usable_connections_changed(&self, _min: usize, _max: usize) {}
    fn in_flight_connections_changed(&self, _min: usize, _max: usize) {}
}

#[cfg(feature = "metrix")]
pub(crate) mod metrix {
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    };
    use std::time::Duration;

    use metrix::cockpit::Cockpit;
    use metrix::instruments::*;
    use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
    use metrix::{TelemetryTransmitter, TransmitsTelemetryData};

    use super::Instrumentation;

    /// A configuration for instrumenting with `metrix`
    pub struct MetrixConfig {
        /// When a `Duration` is set histograms will not report
        /// any values once nothing was changed for the given
        /// duration.
        ///
        /// Default is `None`
        pub histograms_inactivity_timeout: Option<Duration>,
        /// When `histograms_inactivity_timeout` was enabled and
        /// reset is enabled the histogram will reset once it becomes
        /// active again
        ///
        /// Default is `false`
        pub reset_histograms_after_inactivity: bool,
        /// If a `Duration` is set the peak values within the given
        /// duration will be reported.
        ///
        /// Default is enabled with 30 seconds
        pub track_peaks_in_gauges: Option<Duration>,
    }

    impl MetrixConfig {
        /// When a `Duration` is set histograms will not report
        /// any values once nothing was changed for the given
        /// duration.
        ///
        /// Default is `None`
        pub fn histograms_inactivity_timeout(mut self, v: Duration) -> Self {
            self.histograms_inactivity_timeout = Some(v);
            self
        }

        /// When `histograms_inactivity_timeout` was enabled and
        /// reset is enabled the histogram will reset once it becomes
        /// active again
        ///
        /// Default is `false`
        pub fn reset_histograms_after_inactivity(mut self, v: bool) -> Self {
            self.reset_histograms_after_inactivity = v;
            self
        }

        /// If a `Duration` is set the peak values within the given
        /// duration will be reported.
        ///
        /// Default is enabled with 30 seconds
        pub fn track_peaks_in_gauges(mut self, v: Duration) -> Self {
            self.track_peaks_in_gauges = Some(v);
            self
        }

        fn configure_gauge(&self, gauge: &mut Gauge) {
            if let Some(peak_dur) = self.track_peaks_in_gauges {
                gauge.set_peak_keep_alive(peak_dur);
            }
        }

        fn configure_histogram(&self, histogram: &mut Histogram) {
            if let Some(inactivity_dur) = self.histograms_inactivity_timeout {
                histogram.set_inactivity_limit(inactivity_dur);
                histogram.reset_after_inactivity(self.reset_histograms_after_inactivity);
            }
        }
    }

    impl Default for MetrixConfig {
        fn default() -> Self {
            Self {
                histograms_inactivity_timeout: None,
                reset_histograms_after_inactivity: false,
                track_peaks_in_gauges: Some(Duration::from_secs(30)),
            }
        }
    }

    #[derive(Clone, Copy, Eq, PartialEq)]
    pub enum Metric {
        CheckOutConnection,
        CheckedInReturnedConnection,
        CheckedInNewConnection,
        ConnectionDropped,
        ConnectionKilled,
        IdleConnectionsChangedMin,
        IdleConnectionsChangedMax,
        ConnectionCreated,
        ConnectionCreatedTotalTime,
        ReservationsChangedMin,
        ReservationsChangedMax,
        ReservationsChangedLimit,
        ReservationAdded,
        ReservationFulfilled,
        ReservationNotFulfilled,
        ReservationLimitReached,
        ConnectionFactoryFailed,
        UsableConnectionsChangedMin,
        UsableConnectionsChangedMax,
        InFlightConnectionsChangedMin,
        InFlightConnectionsChangedMax,
        LifeTime,
    }

    pub fn create<A: AggregatesProcessors>(
        aggregates_processors: &mut A,
        config: MetrixConfig,
    ) -> MetrixInstrumentation {
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
        let mut histogram = Histogram::new_with_defaults("flight_time_us");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
        cockpit.add_panel(panel);

        let mut panel =
            Panel::with_name(Metric::CheckedInNewConnection, "checked_in_new_connections");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionDropped, "connections_dropped");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        let mut histogram = Histogram::new_with_defaults("flight_time_us");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionKilled, "connections_killed");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::IdleConnectionsChangedMin, "idle_connections_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::IdleConnectionsChangedMax, "idle_connections_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ConnectionCreated, "connections_created");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        let mut histogram = Histogram::new_with_defaults("connect_time_us");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::ConnectionCreatedTotalTime,
            "connections_created_total",
        );
        panel.set_value_scaling(ValueScaling::NanosToMillis);
        let mut histogram = Histogram::new_with_defaults("time_ms");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsChangedMin, "reservations_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsChangedMin, "reservations_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsChangedLimit, "reservations_limit");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationAdded, "reservations_added");
        panel.set_meter(Meter::new_with_defaults("per_second"));
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationFulfilled, "reservations_fulfilled");
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        let mut histogram = Histogram::new_with_defaults("fulfilled_after_us");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::ReservationNotFulfilled,
            "reservations_not_fulfilled",
        );
        panel.set_value_scaling(ValueScaling::NanosToMicros);
        panel.set_meter(Meter::new_with_defaults("per_second"));
        let mut histogram = Histogram::new_with_defaults("not_fulfilled_after_us");
        config.configure_histogram(&mut histogram);
        panel.set_histogram(histogram);
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

        let mut panel = Panel::with_name(
            Metric::UsableConnectionsChangedMin,
            "usable_connections_min",
        );
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::UsableConnectionsChangedMax,
            "usable_connections_max",
        );
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::InFlightConnectionsChangedMin,
            "in_flight_connections_min",
        );
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(
            Metric::InFlightConnectionsChangedMax,
            "in_flight_connections_max",
        );
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();
        rx.add_cockpit(cockpit);

        aggregates_processors.add_processor(rx);

        MetrixInstrumentation::new(tx)
    }

    #[derive(Clone)]
    pub struct MetrixInstrumentation {
        transmitter: TelemetryTransmitter<Metric>,
        limit_sent: Arc<AtomicBool>,
    }

    impl MetrixInstrumentation {
        pub fn new(transmitter: TelemetryTransmitter<Metric>) -> Self {
            Self {
                transmitter,
                limit_sent: Arc::new(AtomicBool::new(false)),
            }
        }
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
        fn idle_connections_changed(&self, min: usize, max: usize) {
            self.transmitter
                .observed_one_value_now(Metric::IdleConnectionsChangedMin, min as u64)
                .observed_one_value_now(Metric::IdleConnectionsChangedMax, max as u64);
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
        fn reservations_changed(&self, min: usize, max: usize, limit: Option<usize>) {
            self.transmitter
                .observed_one_value_now(Metric::ReservationsChangedMin, min as u64)
                .observed_one_value_now(Metric::ReservationsChangedMax, max as u64);

            if let Some(limit) = limit {
                if !self.limit_sent.load(Ordering::SeqCst) {
                    self.limit_sent.store(true, Ordering::SeqCst);
                    self.transmitter
                        .observed_one_value_now(Metric::ReservationsChangedLimit, limit as u64);
                }
            }
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
        fn usable_connections_changed(&self, min: usize, max: usize) {
            self.transmitter
                .observed_one_value_now(Metric::UsableConnectionsChangedMin, min as u64)
                .observed_one_value_now(Metric::UsableConnectionsChangedMax, max as u64);
        }
        fn in_flight_connections_changed(&self, min: usize, max: usize) {
            self.transmitter
                .observed_one_value_now(Metric::InFlightConnectionsChangedMin, min as u64)
                .observed_one_value_now(Metric::InFlightConnectionsChangedMax, max as u64);
        }
    }

}
