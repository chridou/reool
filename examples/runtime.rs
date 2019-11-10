use std::env;

use futures::future::{join_all, TryFutureExt};
use log::{debug, error, info};
use pretty_env_logger;
use tokio::{self, runtime::Runtime};

use reool::{Commands, RedisPool};

/// Do many ping commands with no checkout timeout
/// and an unbounded checkout queue runing on the default runtime
fn main() {
    env::set_var("RUST_LOG", "reool=debug,runtime=debug");
    let _ = pretty_env_logger::try_init();

    let runtime = Runtime::new().unwrap();

    let fut = async {
        let pool = RedisPool::builder()
            .connect_to_node("redis://127.0.0.1:6379")
            .desired_pool_size(10)
            .reservation_limit(None) // No limit
            .checkout_timeout(None) // No timeout
            //.task_executor(runtime.executor()) no explicit executor!
            .finish_redis_rs()
            .unwrap();

        info!("Do one 1000 pings concurrently");
        let futs = (0..1_000)
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

        join_all(futs).await;
        info!("finished pinging");
        info!("PINGED 1000 times concurrently");
        info!("pool goes out of scope");
    };

    runtime.block_on(fut);

    runtime.shutdown_on_idle();
}
