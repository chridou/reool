use std::env;
use std::thread;
use std::time::Duration;

use futures::future::{join_all, Future};
use log::{debug, error, info};
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::instrumentation::StatsLogger;
use reool::{Commands, RedisPool};

/// Do some ping commands and measure the time elapsed
fn main() {
    env::set_var("RUST_LOG", "reool=debug,stats=info");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_nodes(vec![
            "redis://127.0.0.1:6379".to_string(),
            "redis://127.0.0.1:6379".to_string(),
        ])
        .desired_pool_size(3)
        .reservation_limit(None)
        .checkout_timeout(Some(Duration::from_millis(50)))
        .instrumented(StatsLogger)
        .stats_interval(Duration::from_millis(100))
        .task_executor(runtime.executor())
        .redis_rs()
        .unwrap();

    info!("WAIT");
    thread::sleep(Duration::from_secs(1));

    info!("PING 1");
    runtime
        .block_on(pool.check_out().from_err().and_then(Commands::ping))
        .unwrap();

    info!("WAIT");
    thread::sleep(Duration::from_secs(1));

    info!("TRIGGER STATS 1");
    pool.trigger_stats();

    info!("WAIT");
    thread::sleep(Duration::from_secs(1));

    info!("PING 2");
    for _ in 0..10_000 {
        let futs: Vec<_> = (0..7)
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
            debug!("finished pinging");
        });
        runtime.block_on(fut).unwrap();
    }

    thread::sleep(Duration::from_secs(1));
    info!("TRIGGER STATS 2");
    pool.trigger_stats();

    info!("WAIT");
    thread::sleep(Duration::from_secs(1));

    info!("TRIGGER STATS 3");
    pool.trigger_stats();

    info!("WAIT");
    thread::sleep(Duration::from_secs(1));

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
