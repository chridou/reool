use std::{error::Error, time::Instant};

use reool::{RedisOps, RedisPool};

mod connections;
mod data_ops;

pub async fn run(pool: &RedisPool) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let started = Instant::now();
    let mut conn = pool.check_out_default().await?;
    let _ = conn.ping().await?;
    drop(conn);

    flush(pool).await?;

    connections::run(pool).await?;
    flush(pool).await?;
    data_ops::run(pool).await?;
    flush(pool).await?;

    println!("Test run took {:?}", started.elapsed());

    Ok(())
}

async fn flush(pool: &RedisPool) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let mut conn = pool.check_out_default().await?;
    let _ = conn.flushall().await?;

    Ok(())
}
