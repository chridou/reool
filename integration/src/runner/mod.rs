use std::time::{Instant, Duration};

use reool::{error::Error, RedisOps, RedisPool};

mod connections;
mod data_ops;

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    let started = Instant::now();

    let d = pool.ping().await?;
    println!("ping took {:?}", d);

    let pings = pool.ping_nodes(Duration::from_secs(10)).await;
    assert!(pings.iter().all(|p| p.is_ok()));

    pool.flushall().await?;

    connections::run(pool).await?;
    pool.flushall().await?;
    data_ops::run(pool).await?;
    pool.flushall().await?;

    println!("Test run took {:?}", started.elapsed());

    Ok(())
}
