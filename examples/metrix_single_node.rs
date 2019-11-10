use std::env;
use std::time::{Duration, Instant};

use futures::future::{join_all, TryFutureExt};
use log::{debug, error, info};
use metrix::driver::DriverBuilder;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::config::ActivationOrder;
use reool::{Commands, RedisPool};

/// Do many ping commands where many will faile because either
/// the checkout ties out or the checkout queue is full
fn main() {
    env::set_var("RUST_LOG", "reool=debug,metrix_single_node=info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default()
        .set_name("metrix_example")
        .set_driver_metrics(false)
        .build();

    let runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(200)
        .reservation_limit(Some(10_000))
        .checkout_timeout(Some(Duration::from_secs(10)))
        .activation_order(ActivationOrder::LiFo)
        .task_executor(runtime.executor())
        .with_mounted_metrix_instrumentation(&mut driver, Default::default())
        .finish_redis_rs()
        .unwrap();

    // create idle time
    std::thread::sleep(Duration::from_secs(1));

    info!("Do 20000 pings concurrently");
    let futs = (0..20_000)
        .map(|i| {
            let pool = &pool;

            async move {
                let result = pool
                    .check_out()
                    .err_into()
                    .and_then(|mut conn| async move {
                        conn.ping().await
                    });

                match result.await {
                    Err(err) => error!("PING {} failed: {}", i, err),
                    Ok(_) => debug!("PING {} OK", i),
                }
            }
        });

    let start = Instant::now();
    runtime.block_on(async {
        join_all(futs).await;
        info!("finished pinging");
    });
    info!("PINGED 20000 times concurrently in {:?}", start.elapsed());

    let metrics_snapshot = driver.snapshot(false).unwrap();

    println!("{}", metrics_snapshot.to_default_json());

    std::thread::sleep(Duration::from_millis(1500));

    runtime
        .block_on(async {
            let mut conn = pool.check_out().await?;
            conn.ping().await
        })
        .unwrap();

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle();
}
