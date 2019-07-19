# Changelog

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