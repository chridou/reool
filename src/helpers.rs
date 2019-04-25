use std::env;
use std::time::Duration;

use crate::error::{InitializationError, InitializationResult};

fn make_prefix<T: Into<String>>(prefix: Option<T>) -> String {
    prefix
        .map(Into::into)
        .unwrap_or_else(|| "REOOL".to_string())
}

pub fn set_desired_pool_size<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize) -> (),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "DESIRED_POOL_SIZE");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse()
                .map_err(|err| InitializationError::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(InitializationError::new(key, Some(err))),
    }
}

pub fn set_checkout_timeout<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(Option<Duration>) -> (),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "CHECKOUT_TIMEOUT_MS");
    match env::var(&key).map(|s| s.to_uppercase()) {
        Ok(s) => {
            if s == "NONE" {
                f(None);
                Ok(())
            } else {
                f(Some(Duration::from_millis(s.parse().map_err(|err| {
                    InitializationError::new(key, Some(err))
                })?)));
                Ok(())
            }
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(InitializationError::new(key, Some(err))),
    }
}

pub fn set_reservation_limit<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(Option<usize>) -> (),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "RESERVATION_LIMIT");
    match env::var(&key).map(|s| s.to_uppercase()) {
        Ok(s) => {
            if s == "NONE" {
                f(None);
                Ok(())
            } else {
                f(Some(s.parse().map_err(|err| {
                    InitializationError::new(key, Some(err))
                })?));
                Ok(())
            }
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(InitializationError::new(key, Some(err))),
    }
}

pub fn set_min_required_nodes<T, F>(prefix: Option<T>, mut f: F) -> InitializationResult<()>
where
    F: FnMut(usize) -> (),
    T: Into<String>,
{
    let prefix = make_prefix(prefix);

    let key = format!("{}_{}", prefix, "MIN_REQUIRED_NODES");
    match env::var(&key) {
        Ok(s) => {
            f(s.parse()
                .map_err(|err| InitializationError::new(key, Some(err)))?);
            Ok(())
        }
        Err(env::VarError::NotPresent) => Ok(()),
        Err(err) => Err(InitializationError::new(key, Some(err))),
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
        Err(err) => return Err(InitializationError::new(key, Some(err))),
    };

    let parts = parse_connect_to(&s);

    if !parts.is_empty() {
        Ok(Some(parts))
    } else {
        Err(InitializationError::message_only(format!(
            "Found '{}' but it is empty",
            key
        )))
    }
}

fn parse_connect_to(what: &str) -> Vec<String> {
    what.split(';')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim())
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
    assert_eq!(res, vec!["redis://127.0.0.1:6379".to_string(), "redis://127.0.0.1:6380".to_string()]);
}
