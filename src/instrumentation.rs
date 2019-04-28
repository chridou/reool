//! Pluggable instrumentation
use std::time::Duration;

pub use crate::pool::PoolStats;

#[cfg(feature = "metrix")]
pub use self::metrix::MetrixConfig;

/// This instrumentation will do nothing.
pub type NoInstrumentation = ();

/// A trait with methods that get called by the pool on certain events.
///
pub trait Instrumentation {
    /// A connection was checked out
    fn checked_out_connection(&self);

    /// A connection that was previously checked out was checked in again
    fn checked_in_returned_connection(&self, flight_time: Duration);

    /// A newly created connection was checked in
    fn checked_in_new_connection(&self);

    /// A connection was dropped because it was marked as defect
    fn connection_dropped(&self, flight_time: Duration, lifetime: Duration);

    /// A new connection was created
    fn connection_created(&self, connected_after: Duration, total_time: Duration);

    /// A connection was intentionally killed. Happens when connections are removed.
    fn connection_killed(&self, lifetime: Duration);

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

    /// Statistics from the pool
    fn stats(&self, stats: PoolStats);
}

impl Instrumentation for NoInstrumentation {
    fn checked_out_connection(&self) {}
    fn checked_in_returned_connection(&self, _flight_time: Duration) {}
    fn checked_in_new_connection(&self) {}
    fn connection_dropped(&self, _flight_time: Duration, _lifetime: Duration) {}
    fn connection_created(&self, _connected_after: Duration, _total_time: Duration) {}
    fn connection_killed(&self, _lifetime: Duration) {}
    fn reservation_added(&self) {}
    fn reservation_fulfilled(&self, _after: Duration) {}
    fn reservation_not_fulfilled(&self, _after: Duration) {}
    fn reservation_limit_reached(&self) {}
    fn connection_factory_failed(&self) {}
    fn stats(&self, _stats: PoolStats) {}
}

#[cfg(feature = "metrix")]
pub(crate) mod metrix {
    use std::time::Duration;

    use metrix::cockpit::Cockpit;
    use metrix::instruments::*;
    use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
    use metrix::{TelemetryTransmitter, TransmitsTelemetryData};

    use super::Instrumentation;
    use crate::pool::PoolStats;

    /// A configuration for instrumenting with `metrix`
    pub struct MetrixConfig {
        /// When a `Duration` is set all metrics will not report
        /// any values once nothing changed for the given
        /// duration.
        ///
        /// Default is `None`
        pub global_inactivity_timeout: Option<Duration>,
        /// When a `Duration` is set histograms will not report
        /// any values once nothing changed for the given
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
        /// If a `Duration` is set the peak and bottom values within the given
        /// duration will be reported.
        ///
        /// Default is enabled with 30 seconds
        pub track_extrema_in_gauges: Option<Duration>,
    }

    impl MetrixConfig {
        /// When a `Duration` is set all metrics will not report
        /// any values once nothing changed for the given
        /// duration.
        ///
        /// Default is `None`
        pub fn global_inactivity_timeout(mut self, v: Duration) -> Self {
            self.global_inactivity_timeout = Some(v);
            self
        }

        /// When a `Duration` is set histograms will not report
        /// any values once nothing changed for the given
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

        /// If a `Duration` is set the peak and bottom values within the given
        /// duration will be reported.
        ///
        /// Default is enabled with 30 seconds
        pub fn track_extrema_in_gauges(mut self, v: Duration) -> Self {
            self.track_extrema_in_gauges = Some(v);
            self
        }

        fn configure_gauge(&self, gauge: &mut Gauge) {
            if let Some(ext_dur) = self.track_extrema_in_gauges {
                gauge.set_memorize_extrema(ext_dur);
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
                global_inactivity_timeout: None,
                histograms_inactivity_timeout: None,
                reset_histograms_after_inactivity: false,
                track_extrema_in_gauges: Some(Duration::from_secs(30)),
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
        ConnectionCreated,
        ConnectionCreatedTotalTime,
        ReservationAdded,
        ReservationFulfilled,
        ReservationNotFulfilled,
        ReservationLimitReached,
        ConnectionFactoryFailed,
        LifeTime,
        PoolSizeChangedMin,
        PoolSizeChangedMax,
        InFlightConnectionsChangedMin,
        InFlightConnectionsChangedMax,
        IdleConnectionsChangedMin,
        IdleConnectionsChangedMax,
        ReservationsChangedMin,
        ReservationsChangedMax,
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

        let mut panel = Panel::with_name(Metric::PoolSizeChangedMin, "pool_size_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::PoolSizeChangedMax, "pool_size_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::IdleConnectionsChangedMin, "idle_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::IdleConnectionsChangedMax, "idle_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::InFlightConnectionsChangedMin, "in_flight_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::InFlightConnectionsChangedMax, "in_fligh_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsChangedMin, "reservations_min");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let mut panel = Panel::with_name(Metric::ReservationsChangedMax, "reservations_max");
        let mut gauge = Gauge::new_with_defaults("count");
        config.configure_gauge(&mut gauge);
        panel.set_gauge(gauge);
        cockpit.add_panel(panel);

        let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();
        rx.add_cockpit(cockpit);

        if let Some(inactivity_timeout) = config.global_inactivity_timeout {
            rx.set_inactivity_limit(inactivity_timeout)
        }

        aggregates_processors.add_processor(rx);

        MetrixInstrumentation::new(tx)
    }

    #[derive(Clone)]
    pub struct MetrixInstrumentation {
        transmitter: TelemetryTransmitter<Metric>,
    }

    impl MetrixInstrumentation {
        pub fn new(transmitter: TelemetryTransmitter<Metric>) -> Self {
            Self { transmitter }
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
        fn connection_created(&self, connected_after: Duration, total_time: Duration) {
            self.transmitter
                .observed_one_duration_now(Metric::ConnectionCreated, connected_after)
                .observed_one_duration_now(Metric::ConnectionCreatedTotalTime, total_time);
        }
        fn connection_killed(&self, lifetime: Duration) {
            self.transmitter
                .observed_one_now(Metric::ConnectionKilled)
                .observed_one_duration_now(Metric::LifeTime, lifetime);
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

        fn stats(&self, stats: PoolStats) {
            self.transmitter
                .observed_one_value_now(Metric::PoolSizeChangedMin, stats.pool_size.min() as u64)
                .observed_one_value_now(Metric::PoolSizeChangedMax, stats.pool_size.max() as u64);
            self.transmitter
                .observed_one_value_now(
                    Metric::InFlightConnectionsChangedMin,
                    stats.in_flight.min() as u64,
                )
                .observed_one_value_now(
                    Metric::InFlightConnectionsChangedMax,
                    stats.in_flight.max() as u64,
                );
            self.transmitter
                .observed_one_value_now(Metric::IdleConnectionsChangedMin, stats.idle.min() as u64)
                .observed_one_value_now(Metric::IdleConnectionsChangedMax, stats.idle.max() as u64);
            self.transmitter
                .observed_one_value_now(
                    Metric::ReservationsChangedMin,
                    stats.reservations.min() as u64,
                )
                .observed_one_value_now(
                    Metric::ReservationsChangedMax,
                    stats.reservations.max() as u64,
                );
        }
    }
}
