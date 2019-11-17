use std::time::Duration;

use metrix::cockpit::Cockpit;
use metrix::instruments::switches::StaircaseTimer;
use metrix::instruments::*;
use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
use metrix::{TelemetryTransmitter, TransmitsTelemetryData, DECR, INCR};

use super::{Instrumentation, PoolId};

/// A configuration for instrumenting with `metrix`
pub struct MetrixConfig {
    /// When a `Duration` is set all metrics will not report
    /// any values once nothing changed for the given
    /// duration.
    ///
    /// Default is `None`
    pub inactivity_limit: Option<Duration>,
    /// When `inactivity_limit` was enabled and
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
    /// Sets the `Duration` for how long a triggered alert stays `on`
    ///
    /// Default is 60 seconds
    pub alert_duration: Duration,
}

impl MetrixConfig {
    /// When a `Duration` is set all metrics will not report
    /// any values once nothing changed for the given
    /// duration.
    ///
    /// Default is `None`
    pub fn inactivity_limit(mut self, v: Duration) -> Self {
        self.inactivity_limit = Some(v);
        self
    }

    /// When `inactivity_limit` was enabled and
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

    /// Sets the `Duration` for how long a triggered alert stays `on`
    ///
    /// Default is 60 seconds
    pub fn alert_duration(mut self, v: Duration) -> Self {
        self.alert_duration = v;
        self
    }

    fn configure_gauge(&self, gauge: &mut Gauge) {
        if let Some(ext_dur) = self.track_extrema_in_gauges {
            gauge.set_memorize_extrema(ext_dur);
        }
    }

    fn configure_histogram(&self, histogram: &mut Histogram) {
        if let Some(inactivity_limit) = self.inactivity_limit {
            histogram.set_inactivity_limit(inactivity_limit);
            histogram.reset_after_inactivity(self.reset_histograms_after_inactivity);
        }
    }

    fn add_alert<L>(&self, panel: &mut Panel<L>) {
        let mut alert = StaircaseTimer::new_with_defaults("alert");
        alert.set_switch_off_after(self.alert_duration);
        panel.add_instrument(alert);
    }
}

impl Default for MetrixConfig {
    fn default() -> Self {
        Self {
            inactivity_limit: None,
            reset_histograms_after_inactivity: false,
            track_extrema_in_gauges: Some(Duration::from_secs(30)),
            alert_duration: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum Metric {
    CheckOutConnection,
    Fulfillment,
    CheckedInReturnedConnection,
    CheckedInNewConnection,
    ConnectionDropped,
    ConnectionCreated,
    ConnectionCreatedTotalTime,
    ReservationAdded,
    ReservationsChanged,
    ReservationFulfilled,
    ReservationNotFulfilled,
    ReservationLimitReached,
    ConnectionFactoryFailed,
    LifeTime,
    ConnectionsChanged,
    InFlightConnectionsChanged,
    IdleConnectionsChanged,
    PoolCountChanged,

    InternalMessageReceived,
    ExternalMessageReceived,
    ProcessedRelevantMessage,
}

#[derive(Clone)]
pub struct MetrixInstrumentation {
    transmitter: TelemetryTransmitter<Metric>,
}

impl MetrixInstrumentation {
    pub fn new<A: AggregatesProcessors>(
        aggregates_processors: &mut A,
        config: MetrixConfig,
    ) -> Self {
        create(aggregates_processors, config)
    }
}

impl Instrumentation for MetrixInstrumentation {
    fn pool_added(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::PoolCountChanged, INCR);
    }

    fn pool_removed(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::PoolCountChanged, DECR);
    }

    fn checked_out_connection(
        &self,
        idle_for: Duration,
        time_since_checkout_request: Duration,
        _pool: PoolId,
    ) {
        self.transmitter
            .observed_one_duration_now(Metric::CheckOutConnection, idle_for)
            .observed_one_duration_now(Metric::Fulfillment, time_since_checkout_request);
    }

    fn checked_in_returned_connection(&self, flight_time: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(Metric::CheckedInReturnedConnection, flight_time);
    }

    fn checked_in_new_connection(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_now(Metric::CheckedInNewConnection)
            .observed_one_value_now(Metric::ConnectionsChanged, INCR);
    }

    fn connection_dropped(&self, flight_time: Option<Duration>, lifetime: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(
                Metric::ConnectionDropped,
                flight_time.unwrap_or_else(|| Duration::from_secs(0)),
            )
            .observed_one_duration_now(Metric::LifeTime, lifetime)
            .observed_one_value_now(Metric::ConnectionsChanged, DECR);
    }

    fn connection_created(&self, connected_after: Duration, total_time: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(Metric::ConnectionCreated, connected_after)
            .observed_one_duration_now(Metric::ConnectionCreatedTotalTime, total_time);
    }

    fn idle_inc(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::IdleConnectionsChanged, INCR);
    }

    fn idle_dec(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::IdleConnectionsChanged, DECR);
    }

    fn in_flight_inc(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::InFlightConnectionsChanged, INCR);
    }

    fn in_flight_dec(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_value_now(Metric::InFlightConnectionsChanged, DECR);
    }

    fn reservation_added(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_now(Metric::ReservationAdded)
            .observed_one_value_now(Metric::ReservationsChanged, INCR);
    }

    fn reservation_fulfilled(
        &self,
        reservation_time: Duration,
        checkout_request_time: Duration,
        _pool: PoolId,
    ) {
        self.transmitter
            .observed_one_duration_now(Metric::ReservationFulfilled, reservation_time)
            .observed_one_duration_now(Metric::Fulfillment, checkout_request_time)
            .observed_one_value_now(Metric::ReservationsChanged, DECR);
    }

    fn reservation_not_fulfilled(
        &self,
        reservation_time: Duration,
        _checkout_request_time: Duration,
        _pool: PoolId,
    ) {
        self.transmitter
            .observed_one_duration_now(Metric::ReservationNotFulfilled, reservation_time)
            .observed_one_value_now(Metric::ReservationsChanged, DECR);
    }

    fn reservation_limit_reached(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_now(Metric::ReservationLimitReached);
    }

    fn connection_factory_failed(&self, _pool: PoolId) {
        self.transmitter
            .observed_one_now(Metric::ConnectionFactoryFailed);
    }

    fn internal_message_received(&self, latency: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(Metric::InternalMessageReceived, latency);
    }

    fn checkout_message_received(&self, latency: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(Metric::ExternalMessageReceived, latency);
    }

    fn relevant_message_processed(&self, processing_time: Duration, _pool: PoolId) {
        self.transmitter
            .observed_one_duration_now(Metric::ProcessedRelevantMessage, processing_time);
    }
}

fn create<A: AggregatesProcessors>(
    aggregates_processors: &mut A,
    config: MetrixConfig,
) -> MetrixInstrumentation {
    let mut cockpit = Cockpit::without_name(None);

    let mut panel = Panel::with_name(Metric::CheckOutConnection, "checked_out_connections");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    panel.set_meter(Meter::new_with_defaults("per_second"));
    let mut histogram = Histogram::new_with_defaults("idle_time_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::Fulfillment, "fulfillment");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    let mut histogram = Histogram::new_with_defaults("after_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
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

    let mut panel = Panel::with_name(Metric::CheckedInNewConnection, "checked_in_new_connections");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ConnectionDropped, "connections_dropped");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    panel.set_meter(Meter::new_with_defaults("per_second"));
    let mut histogram = Histogram::new_with_defaults("flight_time_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
    config.add_alert(&mut panel);
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

    let mut panel = Panel::with_name(Metric::ReservationLimitReached, "reservation_limit_reached");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    config.add_alert(&mut panel);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ConnectionFactoryFailed, "connection_factory_failed");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    config.add_alert(&mut panel);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::LifeTime, "life_times");
    panel.set_value_scaling(ValueScaling::NanosToMillis);
    panel.set_meter(Meter::new_with_defaults("lifes_ended_per_second"));
    panel.set_histogram(Histogram::new_with_defaults("life_time_ms"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ConnectionsChanged, "connections");
    let mut gauge = Gauge::new_with_defaults("count");
    config.configure_gauge(&mut gauge);
    panel.set_gauge(gauge);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::IdleConnectionsChanged, "idle");
    let mut gauge = Gauge::new_with_defaults("count");
    config.configure_gauge(&mut gauge);
    panel.set_gauge(gauge);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::InFlightConnectionsChanged, "in_flight");
    let mut gauge = Gauge::new_with_defaults("count");
    config.configure_gauge(&mut gauge);
    panel.set_gauge(gauge);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ReservationsChanged, "reservations");
    let mut gauge = Gauge::new_with_defaults("count");
    config.configure_gauge(&mut gauge);
    panel.set_gauge(gauge);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::PoolCountChanged, "pools");
    let mut gauge = Gauge::new_with_defaults("count");
    config.configure_gauge(&mut gauge);
    panel.set_gauge(gauge);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::InternalMessageReceived, "internal_messages");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    panel.set_meter(Meter::new_with_defaults("per_second"));
    let mut histogram = Histogram::new_with_defaults("latency_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ExternalMessageReceived, "external_messages");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    panel.set_meter(Meter::new_with_defaults("per_second"));
    let mut histogram = Histogram::new_with_defaults("latency_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(Metric::ProcessedRelevantMessage, "processed_messages");
    panel.set_value_scaling(ValueScaling::NanosToMicros);
    panel.set_meter(Meter::new_with_defaults("per_second"));
    let mut histogram = Histogram::new_with_defaults("latency_us");
    config.configure_histogram(&mut histogram);
    panel.set_histogram(histogram);
    cockpit.add_panel(panel);

    let (tx, mut rx) = TelemetryProcessor::new_pair_without_name();
    rx.add_cockpit(cockpit);

    if let Some(inactivity_limit) = config.inactivity_limit {
        rx.set_inactivity_limit(inactivity_limit)
    }

    aggregates_processors.add_processor(rx);

    MetrixInstrumentation { transmitter: tx }
}
