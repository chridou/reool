use std::time::{Duration, Instant};

use crate::config::DefaultPoolCheckoutMode;
use crate::{CheckoutMode, Immediately, Wait};

/// The checkout options a pool really has
#[derive(Debug, Copy, Clone)]
pub enum CheckoutConstraint {
    Immediately,
    Until(Instant),
    Wait,
}

impl From<Immediately> for CheckoutConstraint {
    fn from(_: Immediately) -> Self {
        CheckoutConstraint::Immediately
    }
}
impl From<Wait> for CheckoutConstraint {
    fn from(_: Wait) -> Self {
        CheckoutConstraint::Wait
    }
}

impl From<Duration> for CheckoutConstraint {
    fn from(d: Duration) -> Self {
        let timeout = Instant::now() + d;
        timeout.into()
    }
}

impl From<Instant> for CheckoutConstraint {
    fn from(until: Instant) -> Self {
        CheckoutConstraint::Until(until)
    }
}

impl CheckoutConstraint {
    pub fn is_deadline_elapsed(self) -> bool {
        match self {
            CheckoutConstraint::Until(deadline) => deadline < Instant::now(),
            _ => false,
        }
    }

    /// Returns `true` if we can still wait for e.g. another attempt to dispatch
    /// a message to the inner pool
    pub fn can_wait_for_dispatch(&self) -> bool {
        match self {
            CheckoutConstraint::Until(deadline) => *deadline > Instant::now(),
            CheckoutConstraint::Immediately => false,
            CheckoutConstraint::Wait => true,
        }
    }

    pub fn deadline_and_reservation_allowed(self) -> (Option<Instant>, bool) {
        match self {
            CheckoutConstraint::Until(deadline) => (Some(deadline), true),
            CheckoutConstraint::Immediately => (None, false),
            CheckoutConstraint::Wait => (None, true),
        }
    }

    pub fn from_checkout_mode_and_pool_default<T: Into<CheckoutMode>>(
        m: T,
        default: DefaultPoolCheckoutMode,
    ) -> Self {
        match m.into() {
            CheckoutMode::Immediately => CheckoutConstraint::Immediately,
            CheckoutMode::Wait => CheckoutConstraint::Wait,
            CheckoutMode::Until(d) => CheckoutConstraint::Until(d),
            CheckoutMode::PoolDefault => match default {
                DefaultPoolCheckoutMode::Immediately => CheckoutConstraint::Immediately,
                DefaultPoolCheckoutMode::Wait => CheckoutConstraint::Wait,
                DefaultPoolCheckoutMode::WaitAtMost(d) => {
                    CheckoutConstraint::Until(Instant::now() + d)
                }
            },
        }
    }
}
