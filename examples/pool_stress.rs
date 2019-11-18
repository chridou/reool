use std::env;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

use futures::{
    future::{self, Future},
    stream::{self, Stream},
};
use log::info;
use metrix::cockpit::Cockpit;
use metrix::instruments::*;
use metrix::processor::{AggregatesProcessors, TelemetryProcessor};
use metrix::{
    driver::{DriverBuilder, TelemetryDriver},
    TelemetryTransmitter, TransmitsTelemetryData,
};

use pretty_env_logger;
use tokio::runtime::Builder as RuntimeBuilder;
use tokio::timer::Delay;

use reool::connection_factory::*;
use reool::CheckoutErrorKind;
use reool::*;

/// Simply use an artificial connection factory
/// that does not create real connections and hammer the
/// pool with checkout requests.
fn main() {
    env::set_var("RUST_LOG", "info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default().set_driver_metrics(false).build();

    let mut runtime = RuntimeBuilder::new().core_threads(1).build().unwrap();

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
        .pool_multiplier(1)
        .checkout_queue_size(100)
        .retry_on_checkout_limit(true)
        .task_executor(runtime.executor())
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

    let num_clients = 1_000;
    //let delay_dur: Option<Duration> = None;
    let delay_dur: Option<Duration> = Some(Duration::from_millis(15));
    //let checkout_mode = Wait;
    //let checkout_mode = Immediately;
    let checkout_mode = PoolDefault;
    //let checkout_mode = Duration::from_millis(1);

    let clients = future::lazy({
        let running = Arc::clone(&running);
        let pool = pool.clone();
        let collect_result_metrics = collect_result_metrics.clone();
        move || {
            (0..num_clients).for_each(|_n| {
                let running = Arc::clone(&running);
                let pool = pool.clone();
                let collect_result_metrics = collect_result_metrics.clone();
                let f = stream::repeat(()).for_each(move |_| {
                    if !running.load(Ordering::Relaxed) {
                        Box::new(future::err(()))
                    } else {
                        let f = collect_result_metrics
                            .collect(pool.check_out(checkout_mode))
                            .map_err(|_err| ())
                            .and_then(move |_c| {
                                if let Some(delay) = delay_dur {
                                    Box::new(Delay::new(Instant::now() + delay))
                                } else {
                                    Box::new(future::ok(()))
                                        as Box<dyn Future<Item = _, Error = _> + Send>
                                }
                                .map(move |_c| ())
                                .or_else(|_| Ok(()))
                            });
                        Box::new(f) as Box<dyn Future<Item = _, Error = _> + Send>
                    }
                });
                tokio::spawn(f);
            });
            Ok(())
        }
    });

    runtime.spawn(clients);

    thread::sleep(Duration::from_secs(60));
    info!("Finished");
    let state = pool.state();
    drop(pool);
    info!("pool dropped");
    running.store(false, Ordering::Relaxed);
    runtime.shutdown_on_idle().wait().unwrap();
    thread::sleep(Duration::from_secs(2));
    info!("final state:\n{:#?}", state);
    report_stats(&driver);
    info!("=== FINISHED ===");
    thread::sleep(Duration::from_secs(600));
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

    fn create_connection(&self) -> NewConnection<Self::Connection> {
        let count = self.1.fetch_add(1, Ordering::SeqCst);
        NewConnection::new(future::ok(MyConn(count, Arc::clone(&self.0))))
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

    let connections_bottom = snapshot.find("connections/count_bottom");
    let connections_peak = snapshot.find("connections/count_peak");
    info!("connections: {}/{}", connections_bottom, connections_peak);

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
    let mut cockpit = Cockpit::without_name(None);

    let mut panel = Panel::with_name(ResultMetric::Checkout, "checkout");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(ResultMetric::NoConnection, "no_connection");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(ResultMetric::CheckoutTimeout, "checkout_timeout");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(
        ResultMetric::ReservationLimitReached,
        "reservation_limit_reached",
    );
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(ResultMetric::NoPool, "no_pool");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(ResultMetric::CheckoutLimitReached, "checkout_limit_reached");
    panel.set_meter(Meter::new_with_defaults("per_second"));
    cockpit.add_panel(panel);

    let mut panel = Panel::with_name(ResultMetric::TaskExecution, "task_execution");
    panel.set_meter(Meter::new_with_defaults("per_second"));
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
        c: Checkout<T>,
    ) -> impl Future<Item = PoolConnection<T>, Error = CheckoutError> + Send {
        let tx = self.0.clone();
        c.then(move |r| match r {
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
        })
    }
}
