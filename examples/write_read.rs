use std::env;

use log::info;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::{Commands, RedisPool};
use redis::RedisError;

const MY_KEY: &str = "my_key";

/// Write and read a single key.
fn main() {
    env::set_var("RUST_LOG", "reool=debug,write_read=info");
    let _ = pretty_env_logger::try_init();

    let runtime = Runtime::new().unwrap();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(1)
        .task_executor(runtime.executor())
        .finish_redis_rs()
        .unwrap();

    let fut = async {
        let mut conn = pool.check_out().await?;
        let exists = conn.exists(MY_KEY).await?;

        if exists {
            info!("Key already exist");
            conn.del::<_, ()>(MY_KEY).await?;
            info!("key deleted");
        } else {
            info!("Key does not exist");
        }

        conn.set::<_, _, ()>(MY_KEY, "some data").await?;
        info!("data written");

        let data = conn.get::<_, String>(MY_KEY).await?;
        info!("read '{}'", data);

        Ok::<_, RedisError>(data == "some data")
    };

    if runtime.block_on(fut).unwrap() {
        info!("data is equal")
    }

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle();
}
