//! Process-global TMDB request budget.
//!
//! TMDB's free tier caps at ~40 requests / 10 s. The gateway can field many
//! concurrent searches, each wanting to enrich several records, so without a
//! shared throttle a burst of discover queries collectively blows the limit
//! and TMDB starts returning 429s — which surfaces as empty home rows in
//! meta-watch. This is a single hand-rolled token bucket shared by every
//! enrichment task in the process (one `Arc<TmdbBudget>` injected into the
//! torznab plugin via [`meta_feeder_sdk::plugin::FeederPlugin::set_tmdb_budget`]).
//!
//! Design notes:
//! - **Lazy refill.** Tokens accrue on each `acquire` from elapsed wall-clock,
//!   so there is no background timer task and no idle cost.
//! - **Cancel-safe.** The token is decremented synchronously under the lock
//!   right before `acquire` returns [`Lease::Granted`]; there is no `.await`
//!   between the decision to grant and the return, so a dropped `acquire`
//!   future (consumer disconnected) never spends a token.
//! - **Global 429 pause.** A single upstream 429 freezes *all* grants until
//!   the Retry-After window elapses via [`TmdbBudget::note_429`].
//!
//! The critical sections hold a plain `std::sync::Mutex` (no awaits inside),
//! while the waiting happens outside the lock on a `tokio::time::sleep` raced
//! against a `tokio::sync::Notify`.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::Notify;

/// Default sustained TMDB grant rate (requests/second). Held well under
/// TMDB's free-tier ~40 req / 10 s so concurrent searches plus poster-CDN
/// headroom never collectively trip a 429.
pub const DEFAULT_TMDB_REFILL_PER_SEC: f64 = 3.0;
/// Default TMDB burst ceiling (tokens) — a short burst of concurrent searches
/// is served immediately, then throttled to the sustained rate.
pub const DEFAULT_TMDB_BURST: f64 = 6.0;

/// Outcome of an [`TmdbBudget::acquire`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lease {
    /// A token was granted; the caller may make one TMDB API call.
    Granted,
    /// The wait deadline elapsed before a token became available. The caller
    /// should proceed without enrichment (best-effort degrade).
    DeadlineExceeded,
}

struct BucketState {
    tokens: f64,
    capacity: f64,
    refill_per_sec: f64,
    last_refill: Instant,
    /// When set and in the future, all grants are frozen (global 429 pause).
    paused_until: Option<Instant>,
}

impl BucketState {
    /// Accrue tokens for elapsed time since the last refill. Called under the
    /// lock before inspecting `tokens`.
    fn refill(&mut self, now: Instant) {
        let elapsed = now
            .saturating_duration_since(self.last_refill)
            .as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
    }
}

/// Shared token bucket gating TMDB API calls across the whole process.
pub struct TmdbBudget {
    state: Mutex<BucketState>,
    /// Woken whenever a grant *might* now be possible (a token was returned to
    /// the bucket, or a 429 pause was cleared). Waiters re-check under the
    /// lock after waking; a missed wake only costs an extra bounded sleep.
    wake: Notify,
}

impl TmdbBudget {
    /// Build a budget that sustains `refill_per_sec` grants/second with a burst
    /// ceiling of `capacity` tokens. The bucket starts full so the first burst
    /// is served immediately.
    pub fn new(refill_per_sec: f64, capacity: f64) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(BucketState {
                tokens: capacity,
                capacity,
                refill_per_sec,
                last_refill: Instant::now(),
                paused_until: None,
            }),
            wake: Notify::new(),
        })
    }

    /// Wait for one token, giving up after `deadline`. Cancel-safe: dropping
    /// the returned future before it resolves [`Lease::Granted`] spends no
    /// token.
    pub async fn acquire(&self, deadline: Duration) -> Lease {
        let give_up_at = Instant::now() + deadline;
        loop {
            // Decide-or-compute-wait, all under the lock (no awaits inside).
            let sleep_for = {
                let mut st = self.state.lock().expect("tmdb budget mutex poisoned");
                let now = Instant::now();
                if let Some(until) = st.paused_until {
                    if now >= until {
                        st.paused_until = None;
                    }
                }
                let paused = st.paused_until;
                if paused.is_none() {
                    st.refill(now);
                    if st.tokens >= 1.0 {
                        // Atomic grant: decrement and return with no await between.
                        st.tokens -= 1.0;
                        return Lease::Granted;
                    }
                }
                if now >= give_up_at {
                    return Lease::DeadlineExceeded;
                }
                let until_deadline = give_up_at.saturating_duration_since(now);
                let wait = match paused {
                    // Frozen: wake when the pause is scheduled to lift.
                    Some(until) => until.saturating_duration_since(now),
                    // Throttled: wake when the next whole token should accrue.
                    None => {
                        let needed = 1.0 - st.tokens;
                        let secs = if st.refill_per_sec > 0.0 {
                            needed / st.refill_per_sec
                        } else {
                            until_deadline.as_secs_f64()
                        };
                        Duration::from_secs_f64(secs.max(0.0))
                    }
                };
                wait.min(until_deadline)
            };
            tokio::select! {
                _ = tokio::time::sleep(sleep_for) => {}
                _ = self.wake.notified() => {}
            }
            if Instant::now() >= give_up_at {
                return Lease::DeadlineExceeded;
            }
        }
    }

    /// Feed back an upstream 429: pause all grants until `now + retry_after`.
    /// Extends an existing pause but never shortens it.
    pub fn note_429(&self, retry_after: Duration) {
        let until = Instant::now() + retry_after;
        {
            let mut st = self.state.lock().expect("tmdb budget mutex poisoned");
            st.paused_until = Some(match st.paused_until {
                Some(existing) if existing > until => existing,
                _ => until,
            });
        }
        // Wake waiters so they recompute their sleep against the new pause.
        self.wake.notify_waiters();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn grants_up_to_capacity_immediately() {
        let b = TmdbBudget::new(3.0, 3.0);
        // Three tokens available at t0 — all granted without waiting.
        for _ in 0..3 {
            assert_eq!(b.acquire(Duration::from_secs(10)).await, Lease::Granted);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fourth_token_waits_for_refill() {
        let b = TmdbBudget::new(2.0, 2.0); // 2/s sustained
        for _ in 0..2 {
            assert_eq!(b.acquire(Duration::from_secs(10)).await, Lease::Granted);
        }
        // Bucket empty; next grant must wait ~0.5s for one token at 2/s.
        let start = Instant::now();
        assert_eq!(b.acquire(Duration::from_secs(10)).await, Lease::Granted);
        let waited = start.elapsed();
        assert!(
            waited >= Duration::from_millis(450),
            "expected to wait ~0.5s for refill, waited {waited:?}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn deadline_exceeded_when_drained() {
        let b = TmdbBudget::new(0.001, 1.0); // effectively no refill within the test
        assert_eq!(b.acquire(Duration::from_secs(1)).await, Lease::Granted);
        // Bucket empty, refill far slower than the deadline.
        assert_eq!(
            b.acquire(Duration::from_millis(200)).await,
            Lease::DeadlineExceeded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn note_429_pauses_grants_globally() {
        let b = TmdbBudget::new(100.0, 100.0); // plenty of tokens
        b.note_429(Duration::from_secs(2));
        // Even with tokens available, grants are frozen during the pause —
        // a short deadline times out.
        assert_eq!(
            b.acquire(Duration::from_millis(500)).await,
            Lease::DeadlineExceeded
        );
        // After the pause window, grants resume.
        assert_eq!(b.acquire(Duration::from_secs(5)).await, Lease::Granted);
    }
}
