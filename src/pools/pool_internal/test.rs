use std::error::Error as StdError;
use std::fmt;
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};
use std::thread;
use std::time::{Duration, Instant};

use failure::{Fallible, format_err};
use futures::prelude::*;
use future::BoxFuture;
use pretty_env_logger;
use tokio::runtime::Runtime;
use tokio::sync::oneshot;
use tokio::time;

use crate::backoff_strategy::BackoffStrategy;
use crate::error::CheckoutErrorKind;
use crate::executor_flavour::ExecutorFlavour;
use crate::instrumentation::StateCounters;
use crate::pools::pool_internal::{Config, ConnectionFactory, PoolInternal};
use crate::pools::CheckoutConstraint;
use crate::*;

use super::Managed;

async fn check_out_fut<T: Poolable, M: Into<CheckoutConstraint>>(
    pool: &PoolInternal<T>,
    constraint: M,
) -> Result<Managed<T>, CheckoutError> {
    pool.check_out(constraint).await
        .map_err(|failure_package| failure_package.error_kind.into())
}

#[test]
fn given_no_runtime_the_pool_can_sill_be_created() {
    let _pool =
        PoolInternal::no_instrumentation(Config::default(), UnitFactory, ExecutorFlavour::Runtime);
}

#[tokio::test]
async fn given_a_runtime_the_pool_can_be_created() {
    let _ = pretty_env_logger::try_init();

    let (tx, rx) = oneshot::channel();

    tokio::spawn(async {
        let pool = PoolInternal::no_instrumentation(
            Config::default().desired_pool_size(1),
            UnitFactory,
            ExecutorFlavour::Runtime,
        );

        let _ = tx.send(pool);
    });

    let pool = rx.await.unwrap();
    drop(pool);
}

#[test]
fn given_an_explicit_executor_a_pool_can_be_created_and_initialized() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();

    let counters = StateCounters::new();
    let pool = PoolInternal::custom_instrumentation(
        Config::default().desired_pool_size(1),
        UnitFactory,
        runtime.handle().into(),
        counters.instrumentation(),
    );

    thread::sleep(Duration::from_millis(10));

    assert_eq!(counters.pools(), 1, "pools");
    assert_eq!(counters.connections(), 1, "connections");
    assert_eq!(counters.idle(), 1, "idle");
    assert_eq!(counters.in_flight(), 0, "in_flight");
    assert_eq!(counters.reservations(), 0, "reservations");

    drop(pool);
    thread::sleep(Duration::from_millis(20));

    let state = counters.state();

    assert_eq!(state.pools, 0, "pools");
    assert_eq!(state.connections, 0, "connections");
    assert_eq!(state.idle, 0, "idle");
    assert_eq!(state.in_flight, 0, "in_flight");
    assert_eq!(state.reservations, 0, "reservations");
}

#[test]
fn the_pool_shuts_down_cleanly_even_if_connections_cannot_be_created() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();

    let counters = StateCounters::new();
    let pool = PoolInternal::custom_instrumentation(
        Config::default()
            .desired_pool_size(5)
            .backoff_strategy(BackoffStrategy::Constant {
                fixed: Duration::from_millis(1),
                jitter: false,
            }),
        UnitFactoryAlwaysFails,
        runtime.handle().into(),
        counters.instrumentation(),
    );

    thread::sleep(Duration::from_millis(10));
    assert_eq!(counters.pools(), 1, "pools");
    assert_eq!(counters.connections(), 0, "connections");
    assert_eq!(counters.idle(), 0, "idle");
    assert_eq!(counters.in_flight(), 0, "in_flight");
    assert_eq!(counters.reservations(), 0, "reservations");

    drop(pool);

    thread::sleep(Duration::from_millis(10));

    let state = counters.state();

    assert_eq!(state.pools, 0, "pools");
    assert_eq!(state.connections, 0, "connections");
    assert_eq!(state.idle, 0, "idle");
    assert_eq!(state.in_flight, 0, "in_flight");
    assert_eq!(state.reservations, 0, "reservations");
}

#[test]
fn checkout_one() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();
    let config = Config::default().desired_pool_size(1);

    let counters = StateCounters::new();
    let pool = PoolInternal::custom_instrumentation(
        config,
        U32Factory::default(),
        runtime.handle().into(),
        counters.instrumentation(),
    );

    thread::sleep(Duration::from_millis(10));

    let v = runtime.block_on(async {
        let check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.unwrap()
    });

    assert_eq!(v, 0);

    thread::sleep(Duration::from_millis(50));

    drop(pool);
}

#[test]
fn checkout_twice_with_one_not_reusable() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();
    let config = Config::default().desired_pool_size(1);

    let pool = PoolInternal::no_instrumentation(config, U32Factory::default(), runtime.handle().into());

    thread::sleep(Duration::from_millis(10));

    // We do not return the conn with managed by taking it
    let v = runtime.block_on(async {
        let mut check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.take().unwrap()
    });

    assert_eq!(v, 0);

    let v = runtime.block_on(async {
        let check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.unwrap()
    });

    assert_eq!(v, 1);

    drop(pool);
}

#[test]
fn checkout_twice_with_delay_factory_with_one_not_reusable() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();
    let config = Config::default().desired_pool_size(1);

    let pool = PoolInternal::no_instrumentation(config, U32DelayFactory::default(), runtime.handle().into());

    // We do not return the con with managed
    let v = runtime.block_on(async {
        let mut check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.take().unwrap()
    });

    assert_eq!(v, 0);

    let v = runtime.block_on(async {
        let check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.unwrap()
    });

    assert_eq!(v, 1);

    drop(pool);
}

#[test]
fn with_empty_pool_checkout_returns_timeout() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();
    let config = Config::default().desired_pool_size(0);

    let pool = PoolInternal::no_instrumentation(config, UnitFactory, runtime.handle().into());

    let checked_out = check_out_fut(&pool, Duration::from_millis(10));
    let err = runtime.block_on(checked_out).err().unwrap();
    assert_eq!(err.kind(), CheckoutErrorKind::CheckoutTimeout);

    drop(pool);
}

#[test]
fn create_connection_fails_some_times() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();
    let config = Config::default().desired_pool_size(1);

    let pool = PoolInternal::no_instrumentation(
        config,
        U32FactoryFailsThreeTimesInARow::default(),
        runtime.handle().into(),
    );

    thread::sleep(Duration::from_millis(10));

    let v = runtime.block_on(async {
        let mut check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.take().unwrap()
    });

    assert_eq!(v, 4);

    let v = runtime.block_on(async {
        let check_out = check_out_fut(&pool, Wait).await.unwrap();
        check_out.value.unwrap()
    });

    assert_eq!(v, 8);

    drop(pool);
}

#[test]
fn reservations_should_be_fulfilled() {
    (1..=2).for_each(|num_conns| {
        let _ = pretty_env_logger::try_init();
        let runtime = Runtime::new().unwrap();
        let config = Config::default()
            .desired_pool_size(num_conns)
            .reservation_limit(1_000_000);

        let counters = StateCounters::default();
        let pool = Arc::new(PoolInternal::custom_instrumentation(
            config,
            U32Factory::default(),
            runtime.handle().into(),
            counters.instrumentation(),
        ));

        thread::sleep(Duration::from_millis(10));

        while counters.reservations() < 1_000 {
            let pool = pool.clone();

            runtime.spawn(async move {
                check_out_fut(&pool, Wait).await;
            });
        }

        while counters.reservations() != 0 || counters.idle() != num_conns {
            thread::yield_now();
        }

        assert_eq!(counters.pools(), 1, "pools");
        assert_eq!(counters.connections(), num_conns, "connections");
        assert_eq!(counters.idle(), num_conns, "idle");
        assert_eq!(counters.in_flight(), 0, "in_flight");
        assert_eq!(counters.reservations(), 0, "reservations");

        drop(pool);

    });
}

/*
#[test]
fn put_and_checkout_do_not_race() {
    let n = 10000;
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    for _ in 0..n {

        let config = Config::default().desired_pool_size(0);

        let pool = Pool::new(config.clone(), U32Factory::default(), executor);
        let inner_pool = pool.inner_pool().clone();

        thread::spawn(move || {
            let managed = Managed {
                created_at: Instant::now(),
                takeoff_at: None,
                value: Some(0),
                inner_pool: Arc::downgrade(&inner_pool),
                marked_for_kill: false,
            };

            inner_pool.put(managed);
        });

        let checked_out = pool.inner_pool().checkout(None).map(|c| c.value.unwrap());
        let v = checked_out.wait().unwrap();

        assert_eq!(v, 0);

        drop(pool);
    }
}
*/

impl Poolable for () {
    fn connected_to(&self) -> &str {
        ""
    }
}

struct UnitFactory;

impl ConnectionFactory for UnitFactory {
    type Connection = ();

    fn create_connection(&self) -> BoxFuture<Fallible<Self::Connection>> {
        future::ok(()).boxed()
    }

    fn connecting_to(&self) -> &str {
        ""
    }
}

impl Poolable for u32 {
    fn connected_to(&self) -> &str {
        ""
    }
}

struct U32Factory {
    counter: AtomicU32,
}

impl Default for U32Factory {
    fn default() -> Self {
        Self {
            counter: AtomicU32::new(0),
        }
    }
}

impl ConnectionFactory for U32Factory {
    type Connection = u32;
    fn create_connection(&self) -> BoxFuture<Fallible<Self::Connection>> {
        future::ok(self.counter.fetch_add(1, Ordering::SeqCst)).boxed()
    }
    fn connecting_to(&self) -> &str {
        ""
    }
}

struct U32FactoryFailsThreeTimesInARow(AtomicU32);
impl ConnectionFactory for U32FactoryFailsThreeTimesInARow {
    type Connection = u32;

    fn create_connection(&self) -> BoxFuture<Fallible<Self::Connection>> {
        let current_count = self.0.fetch_add(1, Ordering::SeqCst);

        if current_count % 4 == 0 {
            future::ok(current_count).boxed()
        } else {
            future::err(format_err!("hups, no connection")).boxed()
        }
    }

    fn connecting_to(&self) -> &str {
        ""
    }
}
impl Default for U32FactoryFailsThreeTimesInARow {
    fn default() -> Self {
        Self(AtomicU32::new(1))
    }
}

struct UnitFactoryAlwaysFails;
impl ConnectionFactory for UnitFactoryAlwaysFails {
    type Connection = u32;
    fn create_connection(&self) -> BoxFuture<Fallible<Self::Connection>> {
        future::err(format_err!("i have no connections")).boxed()
    }

    fn connecting_to(&self) -> &str {
        ""
    }
}

struct U32DelayFactory {
    counter: AtomicU32,
    delay: Duration,
}

impl Default for U32DelayFactory {
    fn default() -> Self {
        Self {
            counter: AtomicU32::new(0),
            delay: Duration::from_millis(20),
        }
    }
}

impl ConnectionFactory for U32DelayFactory {
    type Connection = u32;

    fn create_connection(&self) -> BoxFuture<Fallible<Self::Connection>> {
        let next = self.counter.fetch_add(1, Ordering::SeqCst);

        async move {
            time::delay_for(self.delay).await;
            Ok(next)
        }
        .boxed()
    }

    fn connecting_to(&self) -> &str {
        ""
    }
}
