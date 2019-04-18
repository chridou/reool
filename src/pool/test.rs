use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

use futures::future::{self, Future};
use pretty_env_logger;
use tokio::runtime::{current_thread, Runtime};
use tokio_timer::Delay;

use crate::error::ErrorKind;
use crate::executor_flavour::ExecutorFlavour;
use crate::pool::{
    BackoffStrategy, Config, ConnectionFactory, NewConnFuture, NewConnectionError, Pool, Poolable,
};

#[test]
#[should_panic]
fn given_no_runtime_the_pool_can_not_be_created() {
    let _pool = Pool::new(Config::default(), UnitFactory, ExecutorFlavour::runtime());
}

#[test]
fn given_a_runtime_the_pool_can_be_created() {
    let _ = pretty_env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let fut = future::lazy(|| {
        Ok::<_, ()>(Pool::new(
            Config::default().max_pool_size(1),
            UnitFactory,
            ExecutorFlavour::runtime(),
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

    let pool = Pool::new(Config::default().max_pool_size(1), UnitFactory, executor);

    thread::sleep(Duration::from_millis(20));

    assert_eq!(1, pool.usable_connections());

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn the_pool_shuts_down_cleanly_even_if_connections_cannot_be_created() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();

    let pool = Pool::new(
        Config::default()
            .max_pool_size(2)
            .backoff_strategy(BackoffStrategy::Constant {
                fixed: Duration::from_millis(1),
                jitter: false,
            }),
        UnitFactoryAlwaysFails,
        executor,
    );

    thread::sleep(Duration::from_millis(10));

    assert_eq!(0, pool.usable_connections());

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn checkout_one() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().max_pool_size(1);

    let pool = Pool::new(config.clone(), U32Factory::default(), executor);

    let checked_out = pool.checkout(None).map(|c| c.managed.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 0);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn checkout_twice_with_one_not_reusable() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().max_pool_size(1);

    let pool = Pool::new(config.clone(), U32Factory::default(), executor);

    // We do not return the con with managed
    let checked_out = pool
        .checkout(None)
        .map(|mut c| c.managed.value.take().unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 0);

    let checked_out = pool.checkout(None).map(|c| c.managed.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 1);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn with_empty_pool_checkout_returns_no_connection() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().max_pool_size(0);

    let pool = Pool::new(config.clone(), U32Factory::default(), executor);

    let checked_out = pool.checkout(Some(Duration::from_millis(10)));
    let err = checked_out.wait().err().unwrap();

    assert_eq!(err.kind(), ErrorKind::NoConnection);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

#[test]
fn create_connection_fails_some_times() {
    let _ = pretty_env_logger::try_init();
    let runtime = Runtime::new().unwrap();
    let executor = runtime.executor().into();
    let config = Config::default().max_pool_size(1);

    let pool = Pool::new(
        config.clone(),
        U32FactoryFailsThreeTimesInARow::default(),
        executor,
    );

    let checked_out = pool
        .checkout(None)
        .map(|mut c| c.managed.value.take().unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 4);

    let checked_out = pool.checkout(None).map(|c| c.managed.value.unwrap());
    let v = checked_out.wait().unwrap();

    assert_eq!(v, 8);

    drop(pool);
    runtime.shutdown_on_idle().wait().unwrap();
}

impl Poolable for () {}

struct UnitFactory;
impl ConnectionFactory for UnitFactory {
    type Connection = ();
    fn create_connection(&self) -> NewConnFuture<Self::Connection> {
        NewConnFuture::new(future::ok(()))
    }
}

impl Poolable for u32 {}
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

struct U32FactoryFailsThreeTimesInARow(AtomicU32);
impl ConnectionFactory for U32FactoryFailsThreeTimesInARow {
    type Connection = u32;
    fn create_connection(&self) -> NewConnFuture<Self::Connection> {
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
            NewConnFuture::new(future::ok(current_count))
        } else {
            NewConnFuture::new(future::err(NewConnectionError::new(MyError)))
        }
    }
}
impl Default for U32FactoryFailsThreeTimesInARow {
    fn default() -> Self {
        Self(AtomicU32::new(1))
    }
}

impl ConnectionFactory for U32Factory {
    type Connection = u32;
    fn create_connection(&self) -> NewConnFuture<Self::Connection> {
        NewConnFuture::new(future::ok(self.counter.fetch_add(1, Ordering::SeqCst)))
    }
}

struct UnitFactoryAlwaysFails;
impl ConnectionFactory for UnitFactoryAlwaysFails {
    type Connection = u32;
    fn create_connection(&self) -> NewConnFuture<Self::Connection> {
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

        NewConnFuture::new(future::err(NewConnectionError::new(MyError)))
    }
}
