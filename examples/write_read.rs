use std::env;
use std::error::Error;

use log::info;
use pretty_env_logger;
use tokio::runtime::Handle;

use reool::AsyncCommands;
use reool::*;

const MY_KEY: &str = "my_key";

/// Write and read a single key.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env::set_var("RUST_LOG", "reool=debug,write_read=info");
    let _ = pretty_env_logger::try_init();

    let pool = RedisPool::builder()
        .connect_to_node("redis://127.0.0.1:6379")
        .desired_pool_size(1)
        .task_executor(Handle::current())
        .finish_redis_rs()
        .unwrap();

    let mut conn = pool.check_out(PoolDefault).await?;
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
    assert_eq!(data, "some data");
    info!("data is equal");

    drop(pool);
    info!("pool dropped");
    Ok(())
}
