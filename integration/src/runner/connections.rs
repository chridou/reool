use std::error::Error;

use reool::{RedisOps, RedisPool};

pub async fn run(pool: &RedisPool) -> Result<(), Box<dyn Error + 'static>> {
    println!("CONNECTIONS");

    println!("Checkout 100 sequentially");
    for _ in 0u32..100 {
        let mut conn = pool.check_out_default().await?;
        conn.ping().await?;
    }

    Ok(())
}
