use std::time::Instant;

use reool::{error::Error, RedisOps, RedisPool};

mod connections;
mod data_ops;

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    let started = Instant::now();

    RedisOps::ping(pool).await?;

    pool.flushall().await?;

    connections::run(pool).await?;
    pool.flushall().await?;
    data_ops::run(pool).await?;
    pool.flushall().await?;

    println!("Test run took {:?}", started.elapsed());

    Ok(())
}
