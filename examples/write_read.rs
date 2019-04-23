use std::env;

use futures::future::{self, Future};
use log::info;
use pretty_env_logger;
use tokio::runtime::Runtime;

use reool::*;

const MY_KEY: &str = "my_key";

/// Write and read a single key.
fn main() {
    env::set_var("RUST_LOG", "reool=debug,write_read=info");
    let _ = pretty_env_logger::try_init();

    let mut runtime = Runtime::new().unwrap();

    let pool = SingleNodePool::builder()
        .connect_to("redis://127.0.0.1:6379")
        .desired_pool_size(1)
        .task_executor(runtime.executor())
        .finish()
        .unwrap();

    let fut = pool.check_out().from_err().and_then(|conn| {
        conn.exists(MY_KEY).and_then(|(conn, exists)| {
            if exists {
                info!("Key already exist");
                Box::new(conn.del::<_, ()>(MY_KEY).map(|(conn, _)| {
                    info!("key deleted");
                    conn
                })) as Box<dyn Future<Item = _, Error = _> + Send>
            } else {
                info!("Key does not exist");
                Box::new(future::ok(conn)) as Box<dyn Future<Item = _, Error = _> + Send>
            }
            .and_then(|conn: PooledConnection| {
                conn.set::<_, _, ()>(MY_KEY, "some data")
                    .and_then(|(conn, _)| {
                        info!("data written");
                        conn.get::<_, String>(MY_KEY).map(|(_, data)| {
                            info!("read '{}'", data);
                            data == "some data"
                        })
                    })
            })
        })
    });

    if runtime.block_on(fut).unwrap() {
        info!("data is equal")
    }

    drop(pool);
    info!("pool dropped");
    runtime.shutdown_on_idle().wait().unwrap();
}
