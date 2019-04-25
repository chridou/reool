# Changelog

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