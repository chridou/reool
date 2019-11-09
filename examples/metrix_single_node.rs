use std::env;
use std::time::{Duration, Instant};

use futures::future::{join_all, Future};
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

    let mut runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(200)
        .reservation_limit(Some(10_000))
        .checkout_mode(Some(Duration::from_secs(10)))
        .activation_order(ActivationOrder::LiFo)
        .task_executor(runtime.executor())
        .with_mounted_metrix_instrumentation(&mut driver, Default::default())
        .finish_redis_rs()
        .unwrap();

    // create idle time
    std::thread::sleep(Duration::from_secs(1));

    info!("Do 20000 pings concurrently");
    let futs: Vec<_> = (0..20_000)
        .map(|i| {
            pool.check_out_pool_default()
                .from_err()
                .and_then(Commands::ping)
                .then(move |res| match res {
                    Err(err) => {
                        error!("PING {} failed: {}", i, err);
                        Ok(())
                    }
                    Ok(_) => {
                        debug!("PING {} OK", i);
                        Ok::<_, ()>(())
                    }
                })
        })
        .collect();

    let fut = join_all(futs).map(|_| {
        info!("finished pinging");
    });

    let start = Instant::now();
    runtime.block_on(fut).unwrap();
    info!("PINGED 20000 times concurrently in {:?}", start.elapsed());

    let metrics_snapshot = driver.snapshot(false).unwrap();

    println!("{}", metrics_snapshot.to_default_json());

    std::thread::sleep(Duration::from_millis(1500));

    runtime
        .block_on(
            pool.check_out_pool_default()
                .from_err()
                .and_then(Commands::ping),
        )
        .unwrap();

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
