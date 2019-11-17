use std::env;

use futures::future::{join_all, lazy, Future};
use log::{debug, error, info};
use pretty_env_logger;
use tokio::{self, runtime::Runtime};

use reool::*;

/// Do many ping commands with no checkout timeout
/// and an unbounded checkout queue runing on the default runtime
fn main() {
    env::set_var("RUST_LOG", "reool=debug,runtime=debug");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let fut = lazy(|| {
        let pool = RedisPool::builder()
            .connect_to_node("redis://127.0.0.1:6379")
            .desired_pool_size(10)
            .reservation_limit(1_000_000)
            .default_checkout_mode(Immediately) // No timeout
            //.task_executor(runtime.executor()) no explicit executor!
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

        fut.map(move |_| pool)
    })
    .map(|_| {
        info!("PINGED 1000 times concurrently");
        info!("pool goes out of scope");
    })
    .map_err(|_| ());

    runtime.block_on(fut).unwrap();

    runtime.shutdown_on_idle().wait().unwrap();
}
