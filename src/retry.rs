use std::time::Duration;

use serde_json::{json, Value};

/// A per-step retry policy: exponential backoff with optional jitter. `max_attempts` counts the
/// first try, so `max_attempts == 1` means "no retry". Mirrors `com.wiggle.core.RetryPolicy`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub initial_backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
    pub jitter: f64,
}

impl RetryPolicy {
    /// Effectively unbounded retries (10,000) every second, capped at a minute. The default for
    /// every step that names no policy of its own.
    pub fn forever() -> Self {
        RetryPolicy {
            max_attempts: 10_000,
            initial_backoff: Duration::from_secs(1),
            multiplier: 1.0,
            max_backoff: Duration::from_secs(60),
            jitter: 0.0,
        }
    }

    /// A single attempt.
    pub fn none() -> Self {
        RetryPolicy {
            max_attempts: 1,
            initial_backoff: Duration::ZERO,
            multiplier: 1.0,
            max_backoff: Duration::ZERO,
            jitter: 0.0,
        }
    }

    /// Up to `max_attempts` tries, doubling from `initial_backoff` (capped at 5 minutes), 20% jitter.
    pub fn exponential(max_attempts: u32, initial_backoff: Duration) -> Self {
        RetryPolicy {
            max_attempts,
            initial_backoff,
            multiplier: 2.0,
            max_backoff: Duration::from_secs(300),
            jitter: 0.2,
        }
    }

    /// Up to `max_attempts` tries with a constant `backoff`.
    pub fn fixed(max_attempts: u32, backoff: Duration) -> Self {
        RetryPolicy { max_attempts, initial_backoff: backoff, multiplier: 1.0, max_backoff: backoff, jitter: 0.0 }
    }

    pub(crate) fn validate(&self) -> Result<(), String> {
        if self.max_attempts < 1 {
            return Err("retry maxAttempts must be >= 1".into());
        }
        if self.multiplier < 1.0 {
            return Err("retry multiplier must be >= 1.0".into());
        }
        Ok(())
    }

    pub(crate) fn to_json(self) -> Value {
        json!({
            "maxAttempts": self.max_attempts,
            "initialBackoffMillis": self.initial_backoff.as_millis() as u64,
            "multiplier": self.multiplier,
            "maxBackoffMillis": self.max_backoff.as_millis() as u64,
            "jitter": self.jitter,
        })
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        RetryPolicy::forever()
    }
}
