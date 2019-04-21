use std::env;
use std::thread;
use std::time::Duration;

use futures::future::Future;
use log::info;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::*;

fn main() {
    env::set_var("RUST_LOG", "reool=debug,connect=info");
    let _ = pretty_env_logger::try_init();

    let runtime = Runtime::new().unwrap();

    let pool = SingleNodePool::builder()
        .task_executor(runtime.executor())
        .connect_to("redis://127.0.0.1:6379")
        .desired_pool_size(5)
        .finish()
        .unwrap();

    info!("{:#?}", pool.stats());

    thread::sleep(Duration::from_secs(1));

    info!("{:#?}", pool.stats());

    drop(pool);
    info!("DROPPED POOL");
    runtime.shutdown_on_idle().wait().unwrap();
}
