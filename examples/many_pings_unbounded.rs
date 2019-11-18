use std::env;
use std::time::Instant;

use futures::future::{join_all, Future};
use log::{debug, error, info};
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::config::DefaultPoolCheckoutMode;
use reool::*;

/// Do many ping commands with no checkout timeout
/// and an unbounded checkout queue
fn main() {
    env::set_var("RUST_LOG", "reool=debug,many_pings_unbounded=debug");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(10)
        .reservation_limit(1_000_000)
        .default_checkout_mode(DefaultPoolCheckoutMode::Immediately)
        .task_executor(runtime.executor())
        .finish_redis_rs()
        .unwrap();

    info!("Do one 1000 pings concurrently");
    let futs: Vec<_> = (0..1_000)
        .map(|i| {
            pool.check_out(PoolDefault)
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
    info!("PINGED 1000 times concurrently in {:?}", start.elapsed());

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
