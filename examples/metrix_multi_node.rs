use std::env;
use std::time::Instant;

use futures::future::{join_all, Future};
use log::{debug, error, info};
use metrix::driver::DriverBuilder;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::multi_node_pool::MultiNodePool;
use reool::{Commands, RedisPool};

/// Do many ping commands where many will faile because either
/// the checkout ties out or the checkout queue is full
fn main() {
    env::set_var("RUST_LOG", "reool=debug,metrix_multi_node=info");
    let _ = pretty_env_logger::try_init();

    let mut driver = DriverBuilder::default()
        .set_name("metrix_example")
        .set_driver_metrics(false)
        .build();

    let mut runtime = Runtime::new().unwrap();

    let pool = MultiNodePool::builder()
        .connect_to(vec![
            "redis://127.0.0.1:6379",
            "redis://127.0.0.1:6379",
            "redis://127.0.0.1:6379",
        ])
        .desired_pool_size(20)
        .reservation_limit(None) // No limit
        .checkout_timeout(None) // No timeout
        .task_executor(runtime.executor())
        .instrumented_with_metrix(&mut driver, Default::default())
        .finish()
        .unwrap();

    info!("Do 20000 pings concurrently");
    let futs: Vec<_> = (0..20_000)
        .map(|i| {
            pool.check_out()
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

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
