// src/throttle.rs
//
// One shared rate limiter for every network stage. Constructed once at
// startup, cloned into ingest / hook / tts / metadata / upload. The point is
// a SINGLE global dial on outbound load no matter which stage is running —
// plus per-host floors so politeness to Gutenberg and Google is independent.
//
// Cargo.toml deps (already in workspace):
// tokio = { version = "1", features = ["full"] }
// rand  = "0.8"

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::{sleep, Instant};

/// Per-host pacing policy. Different services tolerate different cadences:
/// Gutenberg wants ~250ms between hits, Google's token endpoint is fine
/// faster but the upload API gets cranky under bursts.
#[derive(Debug, Clone)]
pub struct HostPolicy {
    pub min_interval: Duration,
}

impl HostPolicy {
    pub fn new(min_interval: Duration) -> Self {
        Self { min_interval }
    }
}

/// Global limiter. `sem` caps total simultaneous in-flight requests across
/// every stage; `hosts` enforces a per-host floor between request *starts*.
/// Cheap to clone — everything inside is an Arc.
#[derive(Clone)]
pub struct Throttle {
    sem: Arc<Semaphore>,
    hosts: Arc<Mutex<HashMap<String, HostState>>>,
    default_policy: HostPolicy,
    policies: Arc<HashMap<String, HostPolicy>>,
}

#[derive(Debug)]
struct HostState {
    last_start: Instant,
    policy: HostPolicy,
}

impl Throttle {
    /// `permits` = global ceiling on concurrent requests. `default_interval`
    /// is the floor applied to any host without an explicit policy.
    pub fn new(permits: usize, default_interval: Duration) -> Self {
        Self {
            sem: Arc::new(Semaphore::new(permits.max(1))),
            hosts: Arc::new(Mutex::new(HashMap::new())),
            default_policy: HostPolicy::new(default_interval),
            policies: Arc::new(HashMap::new()),
        }
    }

    /// Builder: register explicit per-host floors. Keyed by the host string
    /// you pass to `acquire` (e.g. "gutenberg.org", "googleapis.com").
    pub fn with_host_policy(mut self, host: &str, min_interval: Duration) -> Self {
        let mut map = (*self.policies).clone();
        map.insert(host.to_string(), HostPolicy::new(min_interval));
        self.policies = Arc::new(map);
        self
    }

    /// Acquire a slot for a request to `host`. Holds a global permit until the
    /// returned guard drops, and sleeps if firing now would violate that
    /// host's min_interval. The host string is yours to choose — use a stable
    /// key per service so the pacing actually accumulates.
    pub async fn acquire(&self, host: &str) -> ThrottleGuard {
        // 1) global concurrency gate
        let permit = self
            .sem
            .clone()
            .acquire_owned()
            .await
            .expect("throttle semaphore closed");

        // 2) per-host inter-request floor
        let policy = self
            .policies
            .get(host)
            .cloned()
            .unwrap_or_else(|| self.default_policy.clone());

        if !policy.min_interval.is_zero() {
            let mut hosts = self.hosts.lock().await;
            let entry = hosts.entry(host.to_string()).or_insert_with(|| HostState {
                // start in the past so the first hit to a host doesn't wait
                last_start: Instant::now() - policy.min_interval,
                policy: policy.clone(),
            });

            let elapsed = entry.last_start.elapsed();
            if elapsed < entry.policy.min_interval {
                let wait = entry.policy.min_interval - elapsed;
                // drop the lock before sleeping so other hosts aren't blocked
                drop(hosts);
                sleep(wait).await;
                // re-stamp after the wait
                let mut hosts = self.hosts.lock().await;
                if let Some(e) = hosts.get_mut(host) {
                    e.last_start = Instant::now();
                }
            } else {
                entry.last_start = Instant::now();
            }
        }

        ThrottleGuard { _permit: permit }
    }

    /// Back off after a 429 / 503. Sleeps `base * 2^attempt` capped at `max`,
    /// with full jitter so concurrent workers don't re-fire in lockstep.
    /// Does NOT hold a permit — call this between acquire attempts, not during.
    pub async fn backoff(&self, attempt: u32, base: Duration, max: Duration) {
        let exp = base.saturating_mul(1u32 << attempt.min(16));
        let capped = exp.min(max);
        // full jitter: random in [0, capped]
        let millis = capped.as_millis() as u64;
        let jittered = if millis == 0 {
            0
        } else {
            fastrand_u64() % (millis + 1)
        };
        sleep(Duration::from_millis(jittered)).await;
    }
}

/// Tiny self-contained PRNG so we don't pull `rand` in if you'd rather not.
/// Swap for `rand::thread_rng().gen_range(..)` if `rand` is already in tree.
fn fastrand_u64() -> u64 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};
    thread_local! {
        static SEED: Cell<u64> = Cell::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9E3779B97F4A7C15)
                | 1
        );
    }
    SEED.with(|s| {
        // xorshift64*
        let mut x = s.get();
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        s.set(x);
        x.wrapping_mul(0x2545F4914F6CDD1D)
    })
}

/// Held for the lifetime of a single request. Dropping it returns the global
/// permit to the pool. Keep it in scope for the whole send+receive.
pub struct ThrottleGuard {
    _permit: tokio::sync::OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_hit_to_host_does_not_wait() {
        let t = Throttle::new(4, Duration::from_millis(200));
        let start = Instant::now();
        let _g = t.acquire("gutenberg.org").await;
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    #[tokio::test]
    async fn second_hit_same_host_is_paced() {
        let t = Throttle::new(4, Duration::from_millis(150));
        let _g1 = t.acquire("api.example").await;
        drop(_g1);
        let start = Instant::now();
        let _g2 = t.acquire("api.example").await;
        // should have slept roughly the interval
        assert!(start.elapsed() >= Duration::from_millis(120));
    }

    #[tokio::test]
    async fn different_hosts_dont_block_each_other() {
        let t = Throttle::new(4, Duration::from_millis(300));
        let _a = t.acquire("host-a").await;
        let start = Instant::now();
        let _b = t.acquire("host-b").await; // first hit to host-b, no wait
        assert!(start.elapsed() < Duration::from_millis(50));
    }
}