# Reool

[![crates.io](https://img.shields.io/crates/v/reool.svg)](https://crates.io/crates/reool)
[![docs.rs](https://docs.rs/reool/badge.svg)](https://docs.rs/reool)
[![downloads](https://img.shields.io/crates/d/reool.svg)](https://crates.io/crates/reool)
[![license-mit](http://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/chridou/reool/blob/master/LICENSE-MIT)
[![license-apache](http://img.shields.io/badge/license-APACHE-blue.svg)](https://github.com/chridou/reool/blob/master/LICENSE-APACHE)

Currently in early development. API will change rapidly.

## About

Reool is a connection pool for Redis based on [redis-rs](https://crates.io/crates/redis).

Currently `reool` is a fixed size connection pool. `Reool` provides an interface for instrumentation.

You should also consider multiplexing instead of a pool based on your needs.

The `PooledConnection` of `reool` implements the `ConnectionLike`
interface of [redis-rs](https://crates.io/crates/redis) for easier integration.

For documentation visit [crates.io](https://crates.io/crates/reool).

## License

Reool is distributed under the terms of both the MIT license and the Apache License (Version
2.0).
See LICENSE-APACHE and LICENSE-MIT for details.
License: Apache-2.0/MIT