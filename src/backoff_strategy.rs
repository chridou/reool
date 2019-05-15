use std::time::Duration;

use rand::prelude::*;

const NEW_CONN_BACKOFFS_MS: &[Duration] = &[
    Duration::from_millis(5),
    Duration::from_millis(10),
    Duration::from_millis(10),
    Duration::from_millis(15),
    Duration::from_millis(30),
    Duration::from_millis(30), // 100
    Duration::from_millis(50),
    Duration::from_millis(50), // 200
    Duration::from_millis(100),
    Duration::from_millis(100),
    Duration::from_millis(100), // 500
    Duration::from_millis(200),
    Duration::from_millis(300), // 1000
    Duration::from_millis(500),
    Duration::from_millis(500), // 2000
    Duration::from_millis(1_000),
    Duration::from_millis(1_000),
    Duration::from_millis(1_000), // 5000
    Duration::from_millis(2_500),
    Duration::from_millis(2_500), // 10000
    Duration::from_millis(5_000),
    Duration::from_millis(5_000), // 20000
    Duration::from_millis(5_000),
    Duration::from_millis(7_500),
    Duration::from_millis(7_500),  // 50000
    Duration::from_millis(10_000), // 60000
];
const MAX_NEW_CONN_BACKOFF_MS: Duration = Duration::from_millis(10_000);

/// A strategy for determining delays betwen retries
///
/// The pool has a fixed size and will try to create all
/// the needed connections with infinite retries. This
/// is the strategy to determine the delays between subsequent
/// retries.
#[derive(Debug, Clone, Copy)]
pub enum BackoffStrategy {
    /// Immediately retry
    NoBackoff,
    /// Retry always after a fixed interval. Maybe with some jitter.
    Constant { fixed: Duration, jitter: bool },
    /// Use incremental backoff. Max is 10s. Maybe with some jitter.
    Incremental { jitter: bool },
    /// Use incremental backoff but not more than `cap`. Max is 10s. Maybe with some jitter.
    /// The maximum is always 10s even if `cap` is greater.
    IncrementalCapped { cap: Duration, jitter: bool },
}

impl BackoffStrategy {
    pub(crate) fn get_next_backoff(&self, attempt: usize) -> Option<Duration> {
        fn calc_backoff(attempt: usize) -> Duration {
            let idx = (if attempt == 0 { 0 } else { attempt - 1 }) as usize;
            if idx < NEW_CONN_BACKOFFS_MS.len() {
                NEW_CONN_BACKOFFS_MS[idx]
            } else {
                MAX_NEW_CONN_BACKOFF_MS
            }
        }

        let (backoff, with_jitter) = match self {
            BackoffStrategy::NoBackoff => return None,
            BackoffStrategy::Constant { fixed, jitter } => (*fixed, *jitter),
            BackoffStrategy::Incremental { jitter } => (calc_backoff(attempt), *jitter),
            BackoffStrategy::IncrementalCapped { cap, jitter } => {
                let uncapped = calc_backoff(attempt);
                let effective = std::cmp::min(uncapped, *cap);
                (effective, *jitter)
            }
        };
        if with_jitter {
            let ms = backoff.as_millis() as u64;
            let effective_jitter = if ms >= 100 {
                let twenty_percent = ms / 5;
                std::cmp::min(twenty_percent, 3_000)
            } else if ms == 1 {
                1
            } else {
                ms / 3
            };

            if effective_jitter != 0 {
                let mut rng = rand::thread_rng();
                let jitter = rng.gen_range(0, effective_jitter);
                Some(backoff + Duration::from_millis(jitter))
            } else {
                Some(backoff)
            }
        } else {
            Some(backoff)
        }
    }
}

impl Default for BackoffStrategy {
    fn default() -> Self {
        BackoffStrategy::IncrementalCapped {
            cap: Duration::from_secs(10),
            jitter: true,
        }
    }
}
