use std::env;
use std::time::Instant;

use future::join_all;
use futures::prelude::*;
use log::{debug, error, info};
use pretty_env_logger;
use tokio::runtime::Handle;

use reool::config::DefaultPoolCheckoutMode;
use reool::error::Error;
use reool::*;

/// Do many ping commands with no checkout timeout
/// and an unbounded checkout queue
#[tokio::main]
async fn main() {
    env::set_var("RUST_LOG", "reool=debug,many_pings_unbounded=debug");
    let _ = pretty_env_logger::try_init();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(10)
        .reservation_limit(1_000_000)
        .default_checkout_mode(DefaultPoolCheckoutMode::Immediately)
        .task_executor(Handle::current())
        .finish_redis_rs()
        .unwrap();

    info!("Do one 1000 pings concurrently");
    let futs = (0..1_000).map(|i| {
        async {
            let mut check_out = pool.check_out(PoolDefault).await?;
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
    info!("PINGED 1000 times concurrently in {:?}", start.elapsed());

    drop(pool);
    info!("pool dropped");
}
