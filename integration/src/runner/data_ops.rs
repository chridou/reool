use std::error::Error;

use reool::{AsyncCommands, RedisOps, RedisPool};
use tokio::spawn;

pub async fn run(pool: &RedisPool) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    println!("DATA OPS");

    let mut conn = pool.check_out_default().await?;
    let db_size = conn.db_size().await?;
    assert_eq!(db_size, 0);
    drop(conn);

    single_items_seq(pool).await?;
    single_items_par(pool).await?;

    Ok(())
}

async fn single_items_seq(pool: &RedisPool) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
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

async fn single_items_par(pool: &RedisPool) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let items: Vec<usize> = (0..1_000).collect();

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool
                .check_out_default()
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            conn.set(i, i)
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            Ok::<_, Box<dyn Error + Send + Sync + 'static>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await??;
    }

    let mut conn = pool.check_out_default().await?;
    let db_size = conn.db_size().await?;
    drop(conn);
    assert_eq!(db_size, 1_000, "single_items_par set");

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool
                .check_out_default()
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            let v: usize = conn
                .get(i)
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            if v != i {
                let err: Box<dyn Error + Send + Sync + 'static> = format!("{} != {}", v, i).into();
                return Err(err);
            }
            Ok::<_, Box<dyn Error + Send + Sync + 'static>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await??;
    }

    let mut conn = pool.check_out_default().await?;
    let db_size = conn.db_size().await?;
    drop(conn);
    assert_eq!(db_size, 1_000, "single_items_par get");

    let mut handles = Vec::new();
    for i in items.iter().copied() {
        let pool = pool.clone();
        let handle = spawn(async move {
            let mut conn = pool
                .check_out_default()
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            conn.del(i)
                .await
                .map_err(|err| Box::new(err) as Box<dyn Error + Send + Sync + 'static>)?;
            Ok::<_, Box<dyn Error + Send + Sync + 'static>>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let _: () = handle.await??;
    }

    let mut conn = pool.check_out_default().await?;
    let db_size = conn.db_size().await?;
    drop(conn);
    assert_eq!(db_size, 0, "single_items_par del");

    Ok(())
}
