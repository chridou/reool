use std::{error::Error, time::Duration};

use reool::{
    config::{Builder, DefaultPoolCheckoutMode},
    RedisPool,
};

mod runner;

#[tokio::main(threaded_scheduler)]
#[cfg(not(feature = "basic_scheduler"))]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    println!("Using THREADED scheduler");
    the_real_main().await?;
    Ok(())
}

#[tokio::main(basic_scheduler)]
#[cfg(feature = "basic_scheduler")]
async fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    println!("Using BASIC scheduler");
    the_real_main().await?;
    Ok(())
}

async fn the_real_main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    run_test_with_pool(1, 1, 1_000).await?;

    Ok(())
}

pub async fn run_test_with_pool(
    pool_size: usize,
    num_pools: u32,
    reservation_limit: usize,
) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let pool = get_builder(pool_size, num_pools, reservation_limit).finish_redis_rs()?;

    println!(
        "=== Test with {} connection(s) in {} pool(s)",
        pool_size, num_pools,
    );

    match runner::run(&pool).await {
        Ok(()) => {
            println!("SUCCESS");
            Ok(())
        }
        Err(err) => {
            println!("ERROR: {}", err);
            Err(err)
        }
    }
}

pub fn get_builder(pool_size: usize, num_pools: u32, reservation_limit: usize) -> Builder {
    let builder = RedisPool::builder()
        .connect_to_node("redis://localhost:6379")
        .desired_pool_size(pool_size)
        .pool_multiplier(num_pools)
        .reservation_limit(reservation_limit)
        .default_checkout_mode(DefaultPoolCheckoutMode::WaitAtMost(Duration::from_secs(1)));

    builder
}
