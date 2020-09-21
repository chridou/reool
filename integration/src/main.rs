use std::time::Duration;

use reool::{
    config::CheckoutStrategy,
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
    let checkout_strategies = &[
        CheckoutStrategy::OneAttempt,
        CheckoutStrategy::TwoAttempts,
        CheckoutStrategy::OneImmediately,
        CheckoutStrategy::OneCycle,
        CheckoutStrategy::TwoCycles,
    ];

    let mut has_error = false;

    for checkout_strategy in checkout_strategies {
        let mut pool = get_builder(pool_size, num_pools, *checkout_strategy).finish_redis_rs()?;

        println!(
            "=== Test with {} connection(s) in {} pool(s) with checkout strategy {:?}",
            pool_size, num_pools, checkout_strategy
        );

        match runner::run(&mut pool).await {
            Ok(()) => {
                println!("SUCCESS");
            }
            Err(err) => {
                println!("ERROR: {}", err);
                has_error = true;
            }
        }
    }

    if has_error {
        Err(Error::message("At least one test failed"))
    } else {
        Ok(())
    }
}

pub fn get_builder(
    pool_size: usize,
    num_pools: u32,
    checkout_strategy: CheckoutStrategy,
) -> Builder {
    RedisPool::builder()
        .connect_to_node("redis://localhost:6379")
        .desired_pool_size(pool_size)
        .pool_multiplier(num_pools)
        .reservation_limit(50_000)
        .checkout_queue_size(10_000)
        .checkout_strategy(checkout_strategy)
        .default_checkout_mode(DefaultPoolCheckoutMode::WaitAtMost(Duration::from_secs(10)))
}
