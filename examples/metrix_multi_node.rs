use std::env;
use std::time::Instant;

use failure::Fallible;
use futures::prelude::*;
use futures::future::join_all;
use log::{debug, error, info};
use metrix::driver::DriverBuilder;
use pretty_env_logger;
use tokio::runtime::Handle;

use reool::*;

/// Do many ping commands where many will fail because either
/// the checkout ties out or the checkout queue is full
#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "reool=debug,metrix_multi_node=info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default()
        .set_name("metrix_example")
        .set_driver_metrics(false)
        .build();

    let pool = RedisPool::builder()
        .connect_to_nodes(vec![
            "redis://127.0.0.1:6379".to_string(),
            "redis://127.0.0.1:6379".to_string(),
            "redis://127.0.0.1:6379".to_string(),
        ])
        .desired_pool_size(10)
        .reservation_limit(1_000)
        .default_checkout_mode(Immediately)
        .task_executor(Handle::current())
        .with_mounted_metrix_instrumentation(&mut driver, Default::default())
        .finish_redis_rs()
        .unwrap();

    info!("Do 10000 pings concurrently");
    let futs = (0..10_000)
        .map(|i|
            async {
                let mut check_out = pool.check_out_default().await?;
                check_out.ping().await?;
                Fallible::Ok(())
            }
            .map(move |res| match res {
                Err(err) => error!("PING {} failed: {}", i, err),
                Ok(()) => debug!("PING {} OK", i),
            })
        );


    let start = Instant::now();

    join_all(futs).await;
    
    info!("PINGED 10000 times concurrently in {:?}", start.elapsed());

    let metrics_snapshot = driver.snapshot(false).unwrap();

    println!("{}", metrics_snapshot.to_default_json());

    drop(pool);
    info!("pool dropped");
}
