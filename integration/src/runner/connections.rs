use reool::{error::Error, RedisOps, RedisPool};

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    println!("CONNECTIONS");

    for _ in 0u32..100 {
        let mut conn = pool.check_out_default().await?;
        conn.ping().await?;
    }

    quit(pool).await?;

    Ok(())
}

async fn quit(pool: &mut RedisPool) -> Result<(), Error> {
    let mut conn = pool.check_out_default().await?;

    // conn.quit().await?;

    let r = conn.ping().await;

    assert!(r.is_err());

    pool.flushall().await?;
    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0);
    Ok(())
}
