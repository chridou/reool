use std::env;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::Duration;

use future::BoxFuture;
use futures::*;
use log::info;
use metrix::cockpit::Cockpit;
use metrix::instruments::*;
use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
use metrix::{
    driver::{DriverBuilder, TelemetryDriver},
    TelemetryTransmitter, TransmitsTelemetryData,
};

use pretty_env_logger;
use tokio::time;

use reool::connection_factory::*;
use reool::error::Error;
use reool::CheckoutErrorKind;
use reool::*;

/// Simply use an artificial connection factory
/// that does not create real connections and hammer the
/// pool with checkout requests.
#[tokio::main(core_threads = 1)]
async fn main() {
    env::set_var("RUST_LOG", "info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default().set_driver_metrics(false).build();

    let pool = RedisPool::builder()
        //.connect_to_nodes(vec!["C1".to_string()])
        //.connect_to_nodes(vec!["C1".to_string(), "C2".to_string()])
        .connect_to_nodes(vec![
            "C1".to_string(),
            "C2".to_string(),
            "C3".to_string(),
            "C4".to_string(),
        ])
        .desired_pool_size(50)
        .reservation_limit(100)
        .checkout_queue_size(100)
        .retry_on_checkout_limit(true)
        .pool_multiplier(1)
        .default_checkout_mode(Duration::from_millis(30))
        .with_mounted_metrix_instrumentation(&mut driver, Default::default())
        .finish(|conn| Ok(MyConnectionFactory(Arc::new(conn), AtomicUsize::new(0))))
        .unwrap();

    let collect_result_metrics = create_result_metrics(&mut driver);

    let running = Arc::new(AtomicBool::new(true));
    let _ = thread::spawn({
        let driver = driver.clone();
        let running = Arc::clone(&running);
        move || {
            thread::sleep(Duration::from_millis(50));

            while running.load(Ordering::Relaxed) {
                report_stats(&driver);
                thread::sleep(Duration::from_secs(5));
            }
        }
    });

    let num_clients = 1_000usize;
    //let delay_dur: Option<Duration> = None;
    let delay_dur: Option<Duration> = Some(Duration::from_millis(15));
    //let checkout_mode = Wait;
    //let checkout_mode = Immediately;
    let checkout_mode = PoolDefault;
    //let checkout_mode = Duration::from_millis(1);

    for _ in 0..num_clients {
        let running = Arc::clone(&running);
        let pool = pool.clone();
        let collect_result_metrics = collect_result_metrics.clone();

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let check_out = pool.check_out(checkout_mode).await;
                if collect_result_metrics.collect(check_out).is_err() {
                    break;
                }

                if let Some(delay) = delay_dur {
                    time::delay_for(delay).await;
                }
            }
        });
    }

    time::delay_for(Duration::from_secs(60)).await;
    info!("Finished");

    let state = pool.state();
    drop(pool);
    info!("pool dropped");

    running.store(false, Ordering::Relaxed);
    time::delay_for(Duration::from_secs(2)).await;

    info!("final state:\n{:#?}", state);
    report_stats(&driver);

    info!("=== FINISHED ===");
}

struct MyConn(usize, Arc<String>);

impl Poolable for MyConn {
    fn connected_to(&self) -> &str {
        &self.1
    }
}

struct MyConnectionFactory(Arc<String>, AtomicUsize);

impl ConnectionFactory for MyConnectionFactory {
    type Connection = MyConn;

    fn create_connection(&self) -> BoxFuture<Result<Self::Connection, Error>> {
        let count = self.1.fetch_add(1, Ordering::SeqCst);
        future::ok(MyConn(count, Arc::clone(&self.0))).boxed()
    }

    fn connecting_to(&self) -> &str {
        &self.0
    }
}

fn report_stats(driver: &TelemetryDriver) {
    let snapshot = driver.snapshot(false).unwrap();

    let checkouts_count = snapshot.find("checked_out_connections/per_second/count");
    let checkouts_per_second = snapshot.find("checked_out_connections/per_second/one_minute/rate");
    info!("checkouts: {}({}/s)", checkouts_count, checkouts_per_second);

    let reservations_bottom = snapshot.find("reservations/count_bottom");
    let reservations_peak = snapshot.find("reservations/count_peak");
    info!(
        "reservations: {}/{}",
        reservations_bottom, reservations_peak
    );

    let connections = snapshot.find("connections/count");
    let connections_bottom = snapshot.find("connections/count_bottom");
    let connections_peak = snapshot.find("connections/count_peak");
    let connections_avg = snapshot.find("connections/count_avg");
    info!(
        "connections(current/bottom/peak/avg): {}/{}/{}/{}",
        connections, connections_bottom, connections_peak, connections_avg
    );

    let internal_messages = snapshot.find("internal_messages");
    let rate = internal_messages.find("per_second/one_minute/rate");
    let quantiles = internal_messages.find("latency_us/quantiles");
    info!(
        "internal messages p50/p99/p999: {}/{}/{} - {}/s",
        quantiles.find("p50").to_duration_microseconds(),
        quantiles.find("p99").to_duration_microseconds(),
        quantiles.find("p999").to_duration_microseconds(),
        rate,
    );

    let external_messages = snapshot.find("checkout_messages");
    let rate = external_messages.find("per_second/one_minute/rate");
    let quantiles = external_messages.find("latency_us/quantiles");
    info!(
        "checkout messages p50/p99/p999: {}/{}/{} - {}/s",
        quantiles.find("p50").to_duration_microseconds(),
        quantiles.find("p99").to_duration_microseconds(),
        quantiles.find("p999").to_duration_microseconds(),
        rate,
    );

    let checkout_results = snapshot.find("checkout_results");
    info!(
        "checked out: {}/s",
        checkout_results.find("checkout/per_second/one_minute/rate")
    );
    info!(
        "no connection: {}/s",
        checkout_results.find("no_connection/per_second/one_minute/rate")
    );
    info!(
        "timeout: {}/s",
        checkout_results.find("checkout_timeout/per_second/one_minute/rate")
    );
    info!(
        "reservation limit reached: {}/s",
        checkout_results.find("reservation_limit_reached/per_second/one_minute/rate")
    );
    info!(
        "checkout limit: {}/s",
        checkout_results.find("checkout_limit_reached/per_second/one_minute/rate")
    );
    info!("=============================================================")
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum ResultMetric {
    Checkout,
    NoConnection,
    CheckoutTimeout,
    ReservationLimitReached,
    NoPool,
    CheckoutLimitReached,
    TaskExecution,
}

fn create_result_metrics(metrix: &mut TelemetryDriver) -> ResultCollector {
    let mut cockpit = Cockpit::without_name();

    let mut panel = Panel::named(ResultMetric::Checkout, "checkout");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(ResultMetric::NoConnection, "no_connection");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(ResultMetric::CheckoutTimeout, "checkout_timeout");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(
        ResultMetric::ReservationLimitReached,
        "reservation_limit_reached",
    );
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(ResultMetric::NoPool, "no_pool");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(ResultMetric::CheckoutLimitReached, "checkout_limit_reached");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::named(ResultMetric::TaskExecution, "task_execution");
    panel.add_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let (tx, mut rx) = TelemetryProcessor::new_pair("checkout_results");
    rx.add_cockpit(cockpit);

    metrix.add_processor(rx);

    ResultCollector(tx)
}

#[derive(Clone)]
struct ResultCollector(TelemetryTransmitter<ResultMetric>);

impl ResultCollector {
    pub fn collect<T: Poolable>(
        &self,
        conn: Result<PoolConnection<T>, CheckoutError>,
    ) -> Result<PoolConnection<T>, CheckoutError> {
        let tx = self.0.clone();

        match conn {
            Ok(conn) => {
                tx.observed_one_now(ResultMetric::Checkout);
                Ok(conn)
            }
            Err(err) => {
                match err.kind() {
                    CheckoutErrorKind::NoConnection => {
                        tx.observed_one_now(ResultMetric::NoConnection)
                    }
                    CheckoutErrorKind::CheckoutTimeout => {
                        tx.observed_one_now(ResultMetric::CheckoutTimeout)
                    }
                    CheckoutErrorKind::ReservationLimitReached => {
                        tx.observed_one_now(ResultMetric::ReservationLimitReached)
                    }
                    CheckoutErrorKind::NoPool => tx.observed_one_now(ResultMetric::NoPool),
                    CheckoutErrorKind::CheckoutLimitReached => {
                        tx.observed_one_now(ResultMetric::CheckoutLimitReached)
                    }
                    CheckoutErrorKind::TaskExecution => {
                        tx.observed_one_now(ResultMetric::TaskExecution)
                    }
                };
                Err(err)
            }
        }
    }
}
