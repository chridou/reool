# Changelog

## 0.30.0
    * bump `redis-rs` to 0.21
## 0.29.0
    * bump `redis-rs` to 0.20

## 0.28.0 (BREAKING CHANGES)
    * update to tokio 1
    * bump `redis-rs` to 0.19
    * MSRV: 1.45

## 0.27.0
    * bump `redis-rs` to 0.17
    * error handling for IO error changed with `redis-rs` 0.17. So this might be considered a breaking change.

## 0.26.1
    * set gauges in `metrix` feature to zero on creation

## 0.26.0 (BREAKING CHANGES)
    * use `AsyncCommands` trait from `redis-rs` and remove own `Commands` trait
    * add trait `RedisOps` to support specialized use cases like ping etc.
    * reexport `redis` crate
    * `RedisPool` implements `ConnectionLike`
    * `RedisPool::ping` was renamed to `RedisPool::ping_nodes` because there is also a simple ping on `RedisOps`

## 0.25.0 (BREAKING CHANGE)
    * drop connections only on IO errors
    * errors are now `Sync`

## 0.24.0 (BREAKING CHANGE)
    * update to `redis-rs` 0.16.

## 0.23.0 (BREAKING CHANGE)
    * Transition to async/await and futures 0.3.

## 0.22.3
    * When there are multiple pools, try to immediately check out on all but the last

## 0.22.2
    * reexport of `redis-rs` types

## 0.22.1 (BREAKING CHANGE)

    * DNS resolution is non-blocking now
    * Unix domain sockets are not supported currently

## 0.22.0 (BREAKING CHANGE)

    * Upgraded `metrix` feature to 0.10 which is incompatible to previous version

## 0.21.4

    * BUGFIX: In flight must be decreased if a checked out connection gets dropped.

## 0.21.3

    * REFACTOR: Avoid unnecessary checkouts

## 0.21.2

    * BUGFIX: Single pool: Inner dropped when one of multiple clones was dropped

## 0.21.1

    * BUGFIX: Pinged failed always

## 0.21.0 (BREAKING CHANGE) - Read the documentation

    * Use a stream for the internals instead of an async lock
    * Using new configuration parameters and env vars
    * Adjusted the metrics
    * Ping works directly on connections

## 0.20.0 (BREAKING CHANGE)
    * `PoolCheckoutMode` became `DefaultPoolCheckoutMode`
    * Use an `Instant` instead of a `Duration` to define a checkout timeout
    * A contention limit can be set which would prevent a pool from checking out a
    connection if the limit is reached

## 0.19.0 (BREAKING CHANGE)
    * Use a struct to configure the reservation behaviour and the pool default behaviour
    in checkout methods.
    * Remove `stats_interval` from the config
    * Add configuration parameter for the default pool checkout behaviour
    to the config. The enum is `PoolCheckoutMode`
    * `PoolPerNode` will only use the given `CheckoutMode` on the last attempt to checkout
    a connection. On previous attempts `CheckoutMode::Immediately` will be used.

## 0.18.2

    * use tokio async `Lock` instead of a blocking mutex.

## 0.18.1

    * Bugfix: recalculation of connections for pool-per-node-multiplier was not done

## 0.18.0 (BREAKING CHANGE)

    * Removed pool stats
    * Instrumentation trait changed
    * Metrix output changed
    * PoolPerNode can create more sub pools to reduce contention

## 0.17.2 (BREAKING CHANGE)

    * added instrumentation for lock contention

## 0.16.2

    * fix bug in selecting connection(was always the same)

## 0.16.1

    * log created pool type as info

## 0.16.0 (BREAKING CHANGES, metrix only)

    * fixed type in metrics label `in_fligh_max` to `in_flight_max` old value still there but wll be removed

## 0.15.0 (BREAKING CHANGES)

    * renamed `pool_size` to `connections` in PoolStats
    * metrix: renamed metric `pool_size` to `connections`

## 0.14.0 (BREAKING CHANGES)

    * Changed the overall construction logic of the pools
    * ConnectionFactory can return connections to different nodes
    * Removed `PoolMode` which was replaced by `NodePoolStrategy`(breaking change)

## 0.13.4

* Ping struct implements `Debug` trait

## 0.13.3

* each node of a pool can be pinged
* pool returns connection strings for its nodes
* added `dbsize` command

## 0.13.2

* made MetrixInstrumentation ctor public

## 0.13.1

* Allow creation of pool without nodes
* Pool without nodes triggers metrics

## 0.13.0 Update to redis 0.13

## 0.12.0 (BREAKING CHANGES)

* Updated redis-rs to 0.12
* Added `dyn`
* Added commands `set_nx` and `mset_nx`

## 0.11.0 (BREAKING CHANGES)

* Updated redis-rs to 0.11

## 0.10.2

* Added some documentation

## 0.10.1

* Builder updates from environment while maintaining the builder chain

## 0.10.0

* only use one struct `RedisPool` as the external interface
* Remove generic parameters

## 0.9.1

* update parking lot

## 0.9.0

* Support LiFo and FiFo mode for reactivating idle connections
* measure connection idle times

## 0.8.0

* BREAKING CHANGES
    * keep closer to the types of redis-rs

## 0.7.0

* BREAKING CHANGES
    * The pool is generic and modules have been reorganized

## 0.6.1

* Fixed typos in some log messages

## 0.6.0

* Metrix supports inactivity tracking for complete panels(requires metrix feature)
* Added alerts to some instrumentation of metrix

## 0.5.7

* Fixed multi node instrumentation to show the correct values

## 0.5.6

* Allow MultiNode pool to have 0 nodes (but emit a warning)
* Instrumentation can be triggered externally

## 0.5.5

* Add and report node count
* Improve accuracy of stats

## 0.5.4

* Give hint on multiplexing in documentation

## 0.5.3

* Fixed metrix checked out per second.

## 0.5.2

* Fixed name in documentation for environment variable for `STATS_INTERVAL`.

## 0.5.1

* MultiPool will fail on creation when not enough clients could be created

## 0.5.0

* Changed error message for `CONNECT_TO` environment variable check
* Pool has a configurable interval to trigger the instruments
* Metrix feature tracks peak and bottom values
* Metrix feature can be disabled completely on inactivity

## 0.4.2

* Struct `ValueTracker` should be private and now is
* Fixed bug in `ValueTracker` only checking for new min values

## 0.4.1

* added special method to finish a builder
with Strings containing the connection info

## 0.4.0

* renamed `ReplicaSetPool` to MultiNodePool(breaking)
* Made `metrix` instrumentation configurable(breaking)

## 0.3.0

* Reworked instrumentation(breaking if used)
* AtomicIsizes in inner_pool to allow "temporary underflow"
* Config from environment

## 0.2.0

* Moved SingleNodePool to its own module(breaking)
* Added a replica set pool
