use std::env;
use std::time::{Duration, Instant};

use futures::future::join_all;
use futures::prelude::*;
use log::{debug, error, info};
use metrix::driver::DriverBuilder;
use pretty_env_logger;
use tokio::runtime::Handle;
use tokio::time;

use reool::config::ActivationOrder;
use reool::error::Error;
use reool::{RedisOps, RedisPool};

/// Do many ping commands where many will faile because either
/// the checkout ties out or the checkout queue is full
#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "reool=debug,metrix_single_node=info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default()
        .set_name("metrix_example")
        .set_driver_metrics(false)
        .build();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(200)
        .reservation_limit(10_000)
        .default_checkout_mode(Duration::from_secs(10))
        .activation_order(ActivationOrder::LiFo)
        .task_executor(Handle::current())
        .with_mounted_metrix_instrumentation(&mut driver, Default::default())
        .finish_redis_rs()
        .unwrap();

    // create idle time
    time::delay_for(Duration::from_secs(1)).await;

    info!("Do 20000 pings concurrently");
    let futs = (0..20_000).map(|i: usize| {
        async {
            let mut check_out = pool.check_out_default().await?;
            check_out.ping().await?;
            Result::<(), Error>::Ok(())
        }
        .map(move |res| match res {
            Err(err) => error!("PING {} failed: {}", i, err),
            Ok(()) => debug!("PING {} OK", i),
        })
    });

    let start = Instant::now();

    join_all(futs).await;

    info!("finished pinging");
    info!("PINGED 20000 times concurrently in {:?}", start.elapsed());

    let metrics_snapshot = driver.snapshot(false).unwrap();

    println!("{}", metrics_snapshot.to_default_json());

    time::delay_for(Duration::from_millis(1_500)).await;

    let mut conn = pool.check_out_default().await.unwrap();
    conn.ping().await.unwrap();

    drop(pool);
    info!("pool dropped");
}
