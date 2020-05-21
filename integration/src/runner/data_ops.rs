use reool::{error::Error, AsyncCommands, RedisOps, RedisPool};
use tokio::spawn;

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    println!("DATA OPS");

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0);

    single_items_seq(pool).await?;
    single_items_par(pool).await?;

    Ok(())
}

async fn single_items_seq(pool: &RedisPool) -> Result<(), Error> {
    let mut conn = pool.check_out_default().await?;

    let v: Option<u32> = conn.get("test_key").await?;
    assert!(v.is_none());
    conn.set("test_key", 0u32).await?;
    let v: Option<u32> = conn.get("test_key").await?;
    assert_eq!(v, Some(0));
    conn.del("test_key").await?;
    let v: Option<u32> = conn.get("test_key").await?;
    assert!(v.is_none());

    Ok(())
}

async fn single_items_par(pool: &mut RedisPool) -> Result<(), Error> {
    let items: Vec<usize> = (0..1_000).collect();

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool.check_out_default().await?;
            conn.set(i, i).await?;
            Ok::<_, Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await.map_err(Error::cause_by)??;
    }

    let db_size = pool.db_size().await?;

    assert_eq!(db_size, 1_000, "single_items_par set");

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool.check_out_default().await?;
            let v: usize = conn.get(i).await?;
            if v != i {
                let err = Error::message(format!("{} != {}", v, i));
                return Err(err);
            }
            Ok::<_, Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await.map_err(Error::cause_by)??;
    }

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 1_000, "single_items_par get");

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let mut pool = pool.clone();
        let handle = spawn(async move {
            pool.del(i).await?;
            Ok::<_, Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await.map_err(Error::cause_by)??;
    }

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0, "single_items_par del");

    Ok(())
}
