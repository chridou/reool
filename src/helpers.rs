use std::env;

use crate::activation_order::ActivationOrder;
use crate::config::*;
use crate::error::{Error, InitializationResult};

fn make_prefix<T: Into<String>>(prefix: Option<T>) -> String {
    prefix
        .map(Into::into)
        .unwrap_or_else(|| "REOOL".to_string())
}

pub fn set_desired_pool_size<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "DESIRED_POOL_SIZE");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_default_checkout_mode<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(DefaultPoolCheckoutMode),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "DEFAULT_POOL_CHECKOUT_MODE");
    match env::var(&key).map(|s| s.to_uppercase()) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_reservation_limit<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "RESERVATION_LIMIT");
    match env::var(&key).map(|s| s.to_uppercase()) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_min_required_nodes<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "MIN_REQUIRED_NODES");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_retry_on_checkout_limit<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(bool),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "RETRY_ON_CHECKOUT_LIMIT");
    match env::var(&key) {
        Ok(s) => {
            f(s.to_lowercase()
                .parse()
                .map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_activation_order<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(ActivationOrder),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "ACTIVATION_ORDER");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn get_connect_to<T>(prefix: Option<T>) -> InitializationResult<Option<Vec<String>>>
where
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "CONNECT_TO");
    let s = match env::var(&key) {
        Ok(s) => s,
        Err(env::VarError::NotPresent) => return Ok(None),
        Err(err) => return Err(Error::new(key, Some(err))),
    };

    let parts = parse_connect_to(&s);

    if !parts.is_empty() {
        Ok(Some(parts))
    } else {
        Err(Error::message(format!("Found '{}' but it is empty", key)))
    }
}

pub fn set_pool_multiplier<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(u32),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "POOL_MULTIPLIER");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_checkout_queue_size<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "CHECKOUT_QUEUE_SIZE");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_default_command_timeout<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(DefaultCommandTimeout),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "DEFAULT_COMMAND_TIMEOUT");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

pub fn set_checkout_strategy<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(CheckoutStrategy),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "CHECKOUT_STRATEGY");
    match env::var(&key).map(|s| s.to_uppercase()) {
        Ok(s) => {
            f(s.parse().map_err(|err| Error::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(Error::new(key, Some(err))),
    }
}

fn parse_connect_to(what: &str) -> Vec<String> {
    what.split(';')
        .filter(|s| !s.is_empty())
        .map(str::trim)
        .map(ToOwned::to_owned)
        .collect()
}

#[test]
fn prefix_reool_is_default() {
    let prefix = make_prefix::<String>(None);

    assert_eq!(prefix, "REOOL");
}
#[test]
fn prefix_can_be_customized() {
    let prefix = make_prefix(Some("TEST"));

    assert_eq!(prefix, "TEST");
}

#[test]
fn parse_connect_to_empty() {
    let res = parse_connect_to("");
    assert_eq!(res, Vec::<String>::new());
}

#[test]
fn parse_connect_to_one() {
    let res = parse_connect_to("redis://127.0.0.1:6379");
    assert_eq!(res, vec!["redis://127.0.0.1:6379".to_string()]);
}

#[test]
fn parse_connect_to_two() {
    let res = parse_connect_to("redis://127.0.0.1:6379;redis://127.0.0.1:6380");
    assert_eq!(
        res,
        vec![
            "redis://127.0.0.1:6379".to_string(),
            "redis://127.0.0.1:6380".to_string()
        ]
    );
}
