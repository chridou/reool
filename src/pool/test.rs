use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use futures::future::{self, Future};
use log::debug;
use pretty_env_logger;
use tokio::runtime::Runtime;
use tokio_timer::Delay;

use crate::backoff_strategy::BackoffStrategy;
use crate::connection_factory::{NewConnection, NewConnectionError};
use crate::error::ErrorKind;
use crate::executor_flavour::ExecutorFlavour;
use crate::pool::{Config, ConnectionFactory, Pool, Poolable};

#[test]
#[should_panic]
fn given_no_runtime_the_pool_can_not_be_created() {
    let _pool = Pool::no_instrumentation(Config::default(), UnitFactory, ExecutorFlavour::Runtime);
}

#[test]
fn given_a_runtime_the_pool_can_be_created() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let fut = future::lazy(|| {
        Ok::<_, ()>(Pool::no_instrumentation(
            Config::default().desired_pool_size(1),
            UnitFactory,
            ExecutorFlavour::Runtime,
        ))
    });

    runtime.block_on(fut).unwrap();

    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn given_an_explicit_executor_a_pool_can_be_created_and_initialized() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();

    let pool = Pool::no_instrumentation(
        Config::default().desired_pool_size(1),
        UnitFactory,
        executor,
    );

    thread::sleep(Duration::from_millis(20));

    assert_eq!(1, pool.stats().pool_size.0, "pool_size");
    assert_eq!(1, pool.stats().idle.0, "idle");
    assert_eq!(0, pool.stats().in_flight.0, "in_flight");
    assert_eq!(0, pool.stats().reservations.0, "reservations");

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn the_pool_shuts_down_cleanly_even_if_connections_cannot_be_created() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();

    let pool = Pool::no_instrumentation(
        Config::default()
            .desired_pool_size(5)
            .backoff_strategy(BackoffStrategy::Constant {
                fixed: Duration::from_millis(1),
                jitter: false,
            }),
        UnitFactoryAlwaysFails,
        executor,
    );

    thread::sleep(Duration::from_millis(10));

    assert_eq!(0, pool.stats().pool_size.0, "pool_size");
    assert_eq!(0, pool.stats().idle.0, "idle");
    assert_eq!(0, pool.stats().in_flight.0, "in_flight");
    assert_eq!(0, pool.stats().reservations.0, "reservations");

    debug!("drop pool");
    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn checkout_one() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().desired_pool_size(1);

    let pool = Pool::no_instrumentation(config.clone(), U32Factory::default(), executor);

    let checked_out = pool.check_out(None).map(|c| c.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 0);

    thread::sleep(Duration::from_millis(50));

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn checkout_twice_with_one_not_reusable() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().desired_pool_size(1);

    let pool = Pool::no_instrumentation(config.clone(), U32Factory::default(), executor);

    // We do not return the con with managed
    let checked_out = pool.check_out(None).map(|mut c| c.value.take().unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 0);

    let checked_out = pool.check_out(None).map(|c| c.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 1);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn checkout_twice_with_delay_factory_with_one_not_reusable() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().desired_pool_size(1);

    let pool = Pool::no_instrumentation(config.clone(), U32DelayFactory::default(), executor);

    // We do not return the con with managed
    let checked_out = pool.check_out(None).map(|mut c| c.value.take().unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 0);

    let checked_out = pool.check_out(None).map(|c| c.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 1);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn with_empty_pool_checkout_returns_timeout() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().desired_pool_size(0);

    let pool = Pool::no_instrumentation(config.clone(), UnitFactory, executor);

    let checked_out = pool.check_out(Some(Duration::from_millis(10)));
    let err = checked_out.wait().err().unwrap();
    assert_eq!(err.kind(), ErrorKind::Timeout);

    let checked_out = pool.check_out(Some(Duration::from_millis(10)));
    let err = checked_out.wait().err().unwrap();
    assert_eq!(err.kind(), ErrorKind::Timeout);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn create_connection_fails_some_times() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().desired_pool_size(1);

    let pool = Pool::no_instrumentation(
        config.clone(),
        U32FactoryFailsThreeTimesInARow::default(),
        executor,
    );

    let checked_out = pool.check_out(None).map(|mut c| c.value.take().unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 4);

    let checked_out = pool.check_out(None).map(|c| c.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 8);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

/*
#[test]
fn put_and_checkout_do_not_race() {
    let n = 10000;
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    for _ in 0..n {
        let executor = runtime.executor().into();
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
    runtime.shutdown_on_idle().wait().unwrap();
}
*/

impl Poolable for () {
    type Error = ();
}

struct UnitFactory;
impl ConnectionFactory for UnitFactory {
    type Connection = ();
    fn create_connection(&self) -> NewConnection<Self::Connection> {
        NewConnection::new(future::ok(()))
    }
}

impl Poolable for u32 {
    type Error = ();
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
    fn create_connection(&self) -> NewConnection<Self::Connection> {
        NewConnection::new(future::ok(self.counter.fetch_add(1, Ordering::SeqCst)))
    }
}

struct U32FactoryFailsThreeTimesInARow(AtomicU32);
impl ConnectionFactory for U32FactoryFailsThreeTimesInARow {
    type Connection = u32;
    fn create_connection(&self) -> NewConnection<Self::Connection> {
        #[derive(Debug)]
        struct MyError;

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(self.description())
            }
        }

        impl StdError for MyError {
            fn description(&self) -> &str {
                "hups, no connection"
            }

            fn cause(&self) -> Option<&StdError> {
                None
            }
        }

        let current_count = self.0.fetch_add(1, Ordering::SeqCst);
        if current_count % 4 == 0 {
            NewConnection::new(future::ok(current_count))
        } else {
            NewConnection::new(future::err(NewConnectionError::new(MyError)))
        }
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
    fn create_connection(&self) -> NewConnection<Self::Connection> {
        #[derive(Debug)]
        struct MyError;

        impl fmt::Display for MyError {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str(self.description())
            }
        }

        impl StdError for MyError {
            fn description(&self) -> &str {
                "i have no connections"
            }

            fn cause(&self) -> Option<&StdError> {
                None
            }
        }

        NewConnection::new(future::err(NewConnectionError::new(MyError)))
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
    fn create_connection(&self) -> NewConnection<Self::Connection> {
        let delay = Delay::new(Instant::now() + self.delay);
        let next = self.counter.fetch_add(1, Ordering::SeqCst);
        NewConnection::new(
            delay
                .map_err(NewConnectionError::new)
                .and_then(move |()| future::ok(next)),
        )
    }
}
