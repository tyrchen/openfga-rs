//! Bounded lock-free request admission and fixed-window rate controls.

use std::{
    collections::hash_map::RandomState,
    fmt,
    hash::{BuildHasher, Hash},
    net::IpAddr,
    num::NonZeroU32,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use openfga_domain::Principal;
use typed_builder::TypedBuilder;

use crate::ApiError;

const SLOT_COUNT: usize = 4_096;
const MAXIMUM_WINDOW: Duration = Duration::from_hours(1);
const MAXIMUM_RATE: u32 = 1_000_000;

/// Low-cardinality request class with an independently configurable principal rate.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum EndpointClass {
    /// Store, model, assertion, and policy administration.
    Administration,
    /// Tuple and changelog reads.
    Read,
    /// Tuple mutations.
    Write,
    /// `Check` and `BatchCheck` authorization queries.
    Check,
    /// Enumeration and Expand operations.
    Enumeration,
}

/// Finite admission rates for authentication and per-principal endpoint classes.
#[derive(Clone, Copy, Debug, TypedBuilder)]
pub struct AdmissionPolicy {
    /// Fixed-window duration.
    #[builder(default = Duration::from_mins(1))]
    pub(crate) window: Duration,
    /// Authentication attempts per socket peer IP and window.
    #[builder(default = nonzero(20_000))]
    pub(crate) authentication_attempts: NonZeroU32,
    /// Failed authentications per socket peer IP and window.
    #[builder(default = nonzero(2_000))]
    pub(crate) authentication_failures: NonZeroU32,
    /// Global emergency ceiling for authentication attempts per window.
    #[builder(default = nonzero(200_000))]
    pub(crate) global_authentication_attempts: NonZeroU32,
    /// Global emergency ceiling for failed authentications per window.
    #[builder(default = nonzero(20_000))]
    pub(crate) global_authentication_failures: NonZeroU32,
    /// Administrative requests per principal and window.
    #[builder(default = nonzero(1_000))]
    pub(crate) administration: NonZeroU32,
    /// Read requests per principal and window.
    #[builder(default = nonzero(10_000))]
    pub(crate) reads: NonZeroU32,
    /// Write requests per principal and window.
    #[builder(default = nonzero(2_000))]
    pub(crate) writes: NonZeroU32,
    /// Check requests per principal and window.
    #[builder(default = nonzero(20_000))]
    pub(crate) checks: NonZeroU32,
    /// Enumeration requests per principal and window.
    #[builder(default = nonzero(1_000))]
    pub(crate) enumeration: NonZeroU32,
}

const fn nonzero(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

impl AdmissionPolicy {
    pub(crate) fn validate(&self) -> Result<(), &'static str> {
        if self.window.is_zero() || self.window > MAXIMUM_WINDOW {
            return Err("admission_window_out_of_range");
        }
        if [
            self.authentication_attempts,
            self.authentication_failures,
            self.global_authentication_attempts,
            self.global_authentication_failures,
            self.administration,
            self.reads,
            self.writes,
            self.checks,
            self.enumeration,
        ]
        .into_iter()
        .any(|limit| limit.get() > MAXIMUM_RATE)
        {
            return Err("admission_rate_out_of_range");
        }
        Ok(())
    }

    const fn limit(&self, class: EndpointClass) -> NonZeroU32 {
        match class {
            EndpointClass::Administration => self.administration,
            EndpointClass::Read => self.reads,
            EndpointClass::Write => self.writes,
            EndpointClass::Check => self.checks,
            EndpointClass::Enumeration => self.enumeration,
        }
    }
}

#[derive(Debug)]
struct RateSlot(AtomicU64);

impl RateSlot {
    const fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn admit(&self, window: u64, limit: NonZeroU32) -> bool {
        let limit = u64::from(limit.get());
        let mut observed = self.0.load(Ordering::Acquire);
        loop {
            let observed_window = observed >> 32;
            let observed_count = observed & u64::from(u32::MAX);
            if observed_window == window && observed_count >= limit {
                return false;
            }
            let next_count = if observed_window == window {
                observed_count.saturating_add(1)
            } else {
                1
            };
            let next = (window << 32) | next_count;
            match self
                .0
                .compare_exchange_weak(observed, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => return true,
                Err(actual) => observed = actual,
            }
        }
    }
}

trait RateClock: fmt::Debug + Send + Sync {
    fn unix_seconds(&self) -> u64;
}

#[derive(Debug)]
struct SystemRateClock;

impl RateClock for SystemRateClock {
    fn unix_seconds(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| duration.as_secs())
    }
}

/// Shared fixed-memory admission controller used by both public transports.
#[derive(Clone)]
pub struct AdmissionControl {
    policy: AdmissionPolicy,
    slots: Arc<[RateSlot]>,
    clock: Arc<dyn RateClock>,
    hash_builder: RandomState,
}

impl AdmissionControl {
    pub(crate) fn new(policy: AdmissionPolicy) -> Result<Self, &'static str> {
        policy.validate()?;
        let slots = (0..SLOT_COUNT)
            .map(|_| RateSlot::new())
            .collect::<Vec<_>>()
            .into();
        Ok(Self {
            policy,
            slots,
            clock: Arc::new(SystemRateClock),
            hash_builder: RandomState::new(),
        })
    }

    pub(crate) fn admit_authentication(&self, peer_ip: IpAddr) -> Result<(), ApiError> {
        self.admit_key((0_u8, "global"), self.policy.global_authentication_attempts)?;
        self.admit_key((1_u8, peer_ip), self.policy.authentication_attempts)
    }

    pub(crate) fn record_authentication_failure(&self, peer_ip: IpAddr) -> Result<(), ApiError> {
        self.admit_key((2_u8, "global"), self.policy.global_authentication_failures)?;
        self.admit_key((3_u8, peer_ip), self.policy.authentication_failures)
    }

    pub(crate) fn admit_principal(
        &self,
        principal: &Principal,
        class: EndpointClass,
    ) -> Result<(), ApiError> {
        self.admit_key((principal.id().as_str(), class), self.policy.limit(class))
    }

    fn admit_key(&self, key: impl Hash, limit: NonZeroU32) -> Result<(), ApiError> {
        let slot_index = usize::try_from(self.hash_builder.hash_one(key) % SLOT_COUNT as u64)
            .map_err(|_| ApiError::overloaded())?;
        let window = self.clock.unix_seconds() / self.policy.window.as_secs();
        let slot = self
            .slots
            .get(slot_index)
            .ok_or_else(ApiError::overloaded)?;
        if slot.admit(window, limit) {
            Ok(())
        } else {
            Err(ApiError::overloaded())
        }
    }
}

impl fmt::Debug for AdmissionControl {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionControl")
            .field("policy", &self.policy)
            .field("slot_count", &self.slots.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{IpAddr, Ipv4Addr},
        sync::{
            Arc,
            atomic::{AtomicU64, Ordering},
        },
    };

    use openfga_domain::{Principal, PrincipalKind};

    use super::{AdmissionControl, AdmissionPolicy, EndpointClass, RateClock};

    #[derive(Debug)]
    struct ManualClock(AtomicU64);

    impl RateClock for ManualClock {
        fn unix_seconds(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    #[test]
    fn test_should_limit_authentication_and_reset_at_window_boundary() -> Result<(), &'static str> {
        let clock = Arc::new(ManualClock(AtomicU64::new(120)));
        let mut control = AdmissionControl::new(
            AdmissionPolicy::builder()
                .authentication_attempts(super::nonzero(2))
                .build(),
        )?;
        control.clock = clock.clone();
        let peer = IpAddr::V4(Ipv4Addr::LOCALHOST);

        assert!(control.admit_authentication(peer).is_ok());
        assert!(control.admit_authentication(peer).is_ok());
        assert!(control.admit_authentication(peer).is_err());
        clock.0.store(180, Ordering::SeqCst);
        assert!(control.admit_authentication(peer).is_ok());
        Ok(())
    }

    #[test]
    fn test_should_isolate_peer_ips_but_enforce_global_emergency_ceiling()
    -> Result<(), &'static str> {
        let mut control = AdmissionControl::new(
            AdmissionPolicy::builder()
                .authentication_attempts(super::nonzero(1))
                .global_authentication_attempts(super::nonzero(3))
                .build(),
        )?;
        control.clock = Arc::new(ManualClock(AtomicU64::new(120)));
        let first = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        let second = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2));
        let third = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 3));

        assert!(control.admit_authentication(first).is_ok());
        assert!(control.admit_authentication(first).is_err());
        assert!(control.admit_authentication(second).is_ok());
        assert!(control.admit_authentication(third).is_err());
        Ok(())
    }

    #[test]
    fn test_should_isolate_principal_endpoint_buckets() -> Result<(), Box<dyn std::error::Error>> {
        let mut control = AdmissionControl::new(
            AdmissionPolicy::builder()
                .checks(super::nonzero(1))
                .reads(super::nonzero(1))
                .build(),
        )?;
        control.clock = Arc::new(ManualClock(AtomicU64::new(120)));
        let anne = Principal::new(PrincipalKind::PresharedKey, "anne".parse()?);
        let bob = Principal::new(PrincipalKind::PresharedKey, "bob".parse()?);

        assert!(control.admit_principal(&anne, EndpointClass::Check).is_ok());
        assert!(
            control
                .admit_principal(&anne, EndpointClass::Check)
                .is_err()
        );
        assert!(control.admit_principal(&anne, EndpointClass::Read).is_ok());
        assert!(control.admit_principal(&bob, EndpointClass::Check).is_ok());
        Ok(())
    }
}
