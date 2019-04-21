use std::env;
use std::thread;
use std::time::{Duration, Instant};

use futures::future::{join_all, Future};
use futures::stream::{iter_ok, Stream};
use log::{debug, error, info};
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::*;

fn main() {
    env::set_var("RUST_LOG", "reool=debug,too_many_pings=debug");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = SingleNodePool::builder()
        .task_executor(runtime.executor())
        .connect_to("redis://127.0.0.1:6379")
        .desired_pool_size(10)
        .wait_queue_limit(Some(500))
        .checkout_timeout(Some(Duration::from_millis(50)))
        .finish()
        .unwrap();


    info!("Do one thousand pings in concurrently");
    let futs: Vec<_> = (0..1_000)
        .map(|i| {
            pool.checkout()
                .from_err()
                .and_then(|conn| conn.ping())
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
    info!("PINGED 1_000 times concurrently in {:?}", start.elapsed());

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
