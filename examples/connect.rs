use std::env;
use std::thread;
use std::time::Duration;

use futures::future::Future;
use log::info;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::RedisPool;

/// Simply connect to redis and establish some connections
fn main() {
    env::set_var("RUST_LOG", "reool=debug,connect=info");
    let _ = pretty_env_logger::try_init();

    let runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(5)
        .task_executor(runtime.executor())
        .finish_redis_rs()
        .unwrap();

    thread::sleep(Duration::from_secs(1));

    drop(pool);
    info!("DROPPED single node pool POOL");

    let pool = RedisPool::builder()
        .connect_to_nodes(vec![
            "redis://127.0.0.1:6379".to_string(),
            "redis://127.0.0.1:6379".to_string(),
        ])
        .desired_pool_size(5)
        .task_executor(runtime.executor())
        .finish_redis_rs()
        .unwrap();

    thread::sleep(Duration::from_secs(1));

    drop(pool);
    info!("DROPPED multi node pool POOL");

    runtime.shutdown_on_idle().wait().unwrap();
}
