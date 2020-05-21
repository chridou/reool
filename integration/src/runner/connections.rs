use reool::{error::Error, RedisOps, RedisPool};

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    println!("CONNECTIONS");

    for _ in 0u32..100 {
        let mut conn = pool.check_out_default().await?;
        conn.ping().await?;
    }

    Ok(())
}
