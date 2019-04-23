# Reool

Currently in early development. 

## About

Reool is a connection pool for Redis based on [redis-rs](https://crates.io/crates/redis).

Currently `reool` is a fixed size connection pool. `Reool` provides an interface for instrumentation. 

The `PooledConnection` of `reool` implements the `ConnectionLike` 
interface of [redis-rs](https://crates.io/crates/redis) for easier integration.

For documentation visit [crates.io](https://crates.io/crates/reool).

## License

Reool is distributed under the terms of both the MIT license and the Apache License (Version
2.0).
See LICENSE-APACHE and LICENSE-MIT for details.
License: Apache-2.0/MIT