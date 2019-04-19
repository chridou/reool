use std::env;
use std::thread;
use std::time::{Duration, Instant};

use futures::future::Future;
use futures::stream::{iter_ok, Stream};
use log::{error, info};
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::*;

fn main() {
    env::set_var("RUST_LOG", "reool=debug,ping=info");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = SingleNodePool::builder()
        .task_executor(runtime.executor())
        .connect_to("redis://127.0.0.1:6379")
        .desired_pool_size(2)
        .checkout_timeout(Some(Duration::from_millis(10)))
        .finish()
        .unwrap();


    info!("Do one ping");
    let start = Instant::now();
    runtime
        .block_on(pool.checkout().from_err().and_then(|conn| conn.ping()))
        .unwrap();
    info!("PINGED in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));

    info!("Do another ping");
    let start = Instant::now();
    runtime
        .block_on(pool.checkout().from_err().and_then(|conn| conn.ping()))
        .unwrap();
    info!("PINGED in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));

    info!("Do one thousand pings in a row");
    let pool_ping = pool.clone();
    let fut = iter_ok(0..1_000)
        .for_each(move |_| {
            pool_ping
                .checkout()
                .from_err()
                .and_then(|conn| conn.ping())
                .map_err(|err| {
                    error!("PING failed: {}", err);
                })
                .map(|_| ())
        })
        .map(|_| {
            info!("finished pinging");
        });

    let start = Instant::now();
    runtime.block_on(fut).unwrap();
    info!("PINGED 1000 times in a row in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));
 
    info!("Do one thousand pings in concurrently");
    let pool_ping = pool.clone();
    let fut = iter_ok(0..1_000)
        .for_each(move |_| {
            pool_ping
                .checkout()
                .from_err()
                .and_then(|conn| conn.ping())
                .map_err(|err| {
                    error!("PING failed: {}", err);
                })
                .map(|_| ())
        })
        .map(|_| {
            info!("finished pinging");
        });

    let start = Instant::now();
    runtime.block_on(fut).unwrap();
    info!("PINGED 1000 times concurrently in {:?}", start.elapsed());

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}
