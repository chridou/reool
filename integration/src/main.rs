use std::time::Duration;

use reool::{
    config::{Builder, DefaultPoolCheckoutMode},
    error::Error,
    RedisPool,
};

mod runner;

#[tokio::main(threaded_scheduler)]
#[cfg(not(feature = "basic_scheduler"))]
async fn main() -> Result<(), Error> {
    println!("Using THREADED scheduler");
    the_real_main().await?;
    Ok(())
}

#[tokio::main(basic_scheduler)]
#[cfg(feature = "basic_scheduler")]
async fn main() -> Result<(), Error> {
    println!("Using BASIC scheduler");
    the_real_main().await?;
    Ok(())
}

async fn the_real_main() -> Result<(), Error> {
    run_test_with_pool(1, 1).await?;
    run_test_with_pool(1, 2).await?;
    run_test_with_pool(1, 5).await?;

    run_test_with_pool(10, 1).await?;
    run_test_with_pool(10, 2).await?;
    run_test_with_pool(10, 5).await?;

    run_test_with_pool(100, 1).await?;
    run_test_with_pool(100, 2).await?;
    run_test_with_pool(100, 5).await?;

    Ok(())
}

pub async fn run_test_with_pool(pool_size: usize, num_pools: u32) -> Result<(), Error> {
    let mut pool = get_builder(pool_size, num_pools).finish_redis_rs()?;

    println!(
        "=== Test with {} connection(s) in {} pool(s)",
        pool_size, num_pools,
    );

    match runner::run(&mut pool).await {
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

pub fn get_builder(pool_size: usize, num_pools: u32) -> Builder {
    let builder = RedisPool::builder()
        .connect_to_node("redis://localhost:6379")
        .desired_pool_size(pool_size)
        .pool_multiplier(num_pools)
        .reservation_limit(50_000)
        .default_checkout_mode(DefaultPoolCheckoutMode::WaitAtMost(Duration::from_secs(10)));

    builder
}
