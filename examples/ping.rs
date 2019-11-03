use std::env;
use std::thread;
use std::time::{Duration, Instant};

use futures::stream::{iter_ok, Stream};
use futures::{
    future::{join_all, Future},
    sync::oneshot,
};
use log::{debug, error, info, warn};
use pretty_env_logger;
use redis::RedisError;
use tokio::runtime::{Runtime, TaskExecutor};

use reool::{stats::PoolStats, Commands, RedisConnection, RedisPool};

const POOL_SIZE: usize = 8;

/// Do some ping commands and measure the time elapsed
fn main() {
    env::set_var("RUST_LOG", "reool=debug,ping=info");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(POOL_SIZE)
        .reservation_limit(None)
        .checkout_timeout(Some(Duration::from_millis(50)))
        .instrumented(MyMetrics)
        .task_executor(runtime.executor())
        .finish_redis_rs()
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

    info!("ping many times");
    let start = Instant::now();
    ping_concurrently(pool.clone(), runtime.executor());
    info!("PINGED many times concurrently in {:?}", start.elapsed());

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}

fn ping_on_pool(pool: &RedisPool) -> impl Future<Item = (RedisConnection, ()), Error = RedisError> {
    pool.check_out().from_err().and_then(do_a_ping)
}

fn do_a_ping<T>(conn: T) -> impl Future<Item = (T, ()), Error = RedisError>
where
    T: Commands,
{
    conn.ping()
}

struct MyMetrics;

impl reool::instrumentation::Instrumentation for MyMetrics {
    fn checked_out_connection(&self, _idle_for: Duration) {}
    fn checked_in_returned_connection(&self, _flight_time: Duration) {}
    fn checked_in_new_connection(&self) {}
    fn connection_dropped(&self, _flight_time: Duration, _lifetime: Duration) {}
    fn connection_created(&self, _connected_after: Duration, _total_time: Duration) {}
    fn connection_killed(&self, _lifetime: Duration) {}
    fn reservation_added(&self) {}
    fn reservation_fulfilled(&self, _after: Duration) {
        info!("reservation fulfilled")
    }
    fn reservation_not_fulfilled(&self, _after: Duration) {
        warn!("reservation NOT fulfilled")
    }
    fn reservation_limit_reached(&self) {}
    fn connection_factory_failed(&self) {}
    fn stats(&self, _stats: PoolStats) {}
}

fn ping_concurrently(pool: RedisPool, executor: TaskExecutor) {
    let handles: Vec<_> = (0..POOL_SIZE * 2)
        .map(|_| {
            let pool = pool.clone();
            let executor = executor.clone();
            thread::spawn(move || ping_sequentially(&pool, &executor))
        })
        .collect();

    handles
        .into_iter()
        .for_each(|handle| handle.join().unwrap());
}

fn ping_sequentially(pool: &RedisPool, executor: &TaskExecutor) {
    (0..1000).for_each(|_| {
        if let Err(err) = oneshot::spawn(
            pool.check_out().from_err().and_then(Commands::ping),
            executor,
        )
        .wait()
        {
            error!("ping failed: {}", err);
        }
    })
}
