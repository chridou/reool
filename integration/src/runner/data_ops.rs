use reool::redis::ErrorKind;
use reool::{error::Error, AsyncCommands, RedisOps, RedisPool};
use tokio::spawn;

pub async fn run(pool: &mut RedisPool) -> Result<(), Error> {
    println!("DATA OPS");

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0);

    single_items_seq(pool).await?;
    single_items_par(pool).await?;
    set_mget(pool).await?;

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
    let items: Vec<usize> = (0..10_000).collect();

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
        let _: () = handle.await.map_err(Error::caused_by)??;
    }

    let db_size = pool.db_size().await?;

    assert_eq!(db_size, 10_000, "single_items_par set");

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
        let _: () = handle.await.map_err(Error::caused_by)??;
    }

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 10_000, "single_items_par get");

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
        let _: () = handle.await.map_err(Error::caused_by)??;
    }

    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0, "single_items_par del");

    Ok(())
}

async fn set_mget(pool: &mut RedisPool) -> Result<(), Error> {
    pool.set("key_1", "1").await?;
    pool.set_multiple(&[("key_2", "2"), ("key_3", "3")]).await?;

    let v: Vec<String> = pool.get(vec!["key_1", "key_2", "key_3"]).await?;

    assert_eq!(v[0], "1");
    assert_eq!(v[1], "2");
    assert_eq!(v[2], "3");

    // This will return 2 values!
    let v: Vec<String> = pool.get(vec!["key_1", "XXX", "key_3"]).await?;

    assert_eq!(v[0], "1");
    assert_eq!(v[1], "3");

    let v: Vec<Option<String>> = pool.get(vec!["key_1", "key_2", "key_3"]).await?;

    assert_eq!(v[0], Some("1".to_string()));
    assert_eq!(v[1], Some("2".to_string()));
    assert_eq!(v[2], Some("3".to_string()));

    let v: Vec<Option<String>> = pool.get(vec!["key_1", "XXX", "key_3"]).await?;

    assert_eq!(v[0], Some("1".to_string()));
    assert_eq!(v[1], None);
    assert_eq!(v[2], Some("3".to_string()));

    // Getting a single value with a vec into a vec causes an error!
    let err = pool.get::<_, Vec<String>>(vec!["key_1"]).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TypeError);

    // but it works when the target signals that it is only 1 value:
    let v: String = pool.get(vec!["key_1"]).await?;
    assert_eq!(v, "1");

    // Getting a non existent value with into a string causes an error!
    let err = pool.get::<_, String>(vec!["XXX"]).await.unwrap_err();
    assert_eq!(err.kind(), ErrorKind::TypeError);

    pool.flushall().await?;
    let db_size = pool.db_size().await?;
    assert_eq!(db_size, 0);
    Ok(())
}
