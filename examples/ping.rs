use std::env;
use std::thread;
use std::time::{Duration, Instant};

use futures::future::{join_all, Future};
use futures::stream::{iter_ok, Stream};
use log::{debug, error, info};
use pretty_env_logger;
use redis::RedisError;
use tokio::runtime::Runtime;

use reool::node_pool::Builder;
use reool::{Commands, PooledConnection, RedisPool};

/// Do some ping commands and measure the time elapsed
fn main() {
    env::set_var("RUST_LOG", "reool=debug,ping=info");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = Builder::default()
        .connect_to("redis://127.0.0.1:6379")
        .desired_pool_size(3)
        .reservation_limit(None)
        .checkout_timeout(Some(Duration::from_millis(50)))
        .task_executor(runtime.executor())
        .redis_rs()
        .unwrap();

    info!("Do one ping");
    let start = Instant::now();
    runtime
        .block_on(pool.check_out().from_err().and_then(Commands::ping))
        .unwrap();
    info!("PINGED in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));

    info!("Do another ping");
    let start = Instant::now();
    runtime.block_on(ping_on_pool(&pool)).unwrap();
    info!("PINGED in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));

    info!("Do one hundred pings in a row");
    let pool_ping = pool.clone();
    let fut = iter_ok(0..100)
        .for_each(move |_| {
            pool_ping
                .check_out()
                .from_err()
                .and_then(do_a_ping)
                .then(|res| match res {
                    Err(err) => {
                        error!("PING failed: {}", err);
                        Ok(())
                    }
                    Ok(_) => Ok::<_, ()>(()),
                })
        })
        .map(|_| {
            info!("finished pinging");
        });

    let start = Instant::now();
    runtime.block_on(fut).unwrap();
    info!("PINGED 100 times in a row in {:?}", start.elapsed());

    thread::sleep(Duration::from_secs(1));

    info!("Do one hundred pings in concurrently");
    let futs: Vec<_> = (0..100)
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
    info!("PINGED 100 times concurrently in {:?}", start.elapsed());

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}

fn ping_on_pool<P: RedisPool>(
    pool: &P,
) -> impl Future<Item = (PooledConnection<P::Connection>, ()), Error = RedisError> {
    pool.check_out().from_err().and_then(do_a_ping)
}

fn do_a_ping<T>(conn: T) -> impl Future<Item = (T, ()), Error = RedisError>
where
    T: Commands,
{
    conn.ping()
}
