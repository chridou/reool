use reool::{error::Error, RedisOps, RedisPool};
use reool::redis::ErrorKind;

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    println!("CONNECTIONS");

    for _ in 0u32..100 {
        let mut conn = pool.check_out_default().await?;
        conn.ping().await?;
    }

    kill_conn(pool).await?;

    Ok(())
}

async fn kill_conn(pool: &mut RedisPool) -> Result<(), Error> {
    let state = pool.state();
    if state.connections == 1 {
        return Ok(())
    }

    let mut conn_to_be_killed = pool.check_out_default().await?;
    let client_id = conn_to_be_killed.client_id().await?;

    let mut killer_conn = pool.check_out_default().await?;
    killer_conn.client_kill_id(client_id).await?;
    drop(killer_conn);
    let mut killed_conn = conn_to_be_killed;

    // The first time we get a response error form redis-rs itself.
    let err = killed_conn.ping().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::ResponseError);
    let err = killed_conn.ping().await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::IoError);

    pool.flushall().await?;
    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0);
    Ok(())
}
