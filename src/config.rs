// src/config.rs
//
// Unified per-niche configuration. One TOML -> one AppConfig.
//
// Design contract:
//   * Every ${ENV_VAR} reference is resolved recursively against the raw TOML
//     string BEFORE parsing, so no downstream module ever touches std::env.
//   * A required field whose ${VAR} is missing/empty is a HARD ERROR at load,
//     not a silent empty string that fails opaquely in upload.rs at 3am.
//   * validate() catches the dumb-but-common misconfigs (empty db_path, bad
//     privacy_status, zero concurrency) up front with a readable message.
//
// Cargo.toml deps:
//   serde  = { version = "1", features = ["derive"] }
//   toml   = "0.8"
//   anyhow = "1"
//   regex  = "1"

use anyhow::{bail, Context, Result};
use regex::Regex;
use serde::Deserialize;
use std::time::Duration;

// Re-export every sub-config so callers do `use crate::config::HookConfig`
// instead of reaching into each module. These live in their own files; here
// we only own the ones with no natural home (channel, throttle, auth shape).
pub use crate::assemble::AssembleConfig;
pub use crate::hook::HookConfig;
pub use crate::ingest::IngestConfig;
pub use crate::metadata::MetadataConfig;
pub use crate::thumbnail::ThumbnailConfig;
pub use crate::tts::TtsConfig;
pub use crate::upload::UploadConfig;

// ============================================================
//  Top-level config
// ============================================================

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    /// Path to the SQLite file. Required, non-empty.
    pub db_path: String,

    /// How many times a per-book stage may fail before it's dead-lettered.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: i64,

    pub channel: ChannelConfig,
    pub throttle: ThrottleConfig,
    pub auth: AuthConfig,

    pub ingest: IngestConfig,
    pub hook: HookConfig,
    pub tts: TtsConfig,
    pub assemble: AssembleConfig,
    pub metadata: MetadataConfig,
    pub thumbnail: ThumbnailConfig,
    pub upload: UploadConfig,
    pub selector: SelectorConfig,
}

fn default_max_attempts() -> i64 { 3 }

/// `[selector]` — the explore/exploit budget allocator's tuning knobs.
#[derive(Debug, Clone, Deserialize)]
pub struct SelectorConfig {
    pub total_budget: i64,
    pub min_per_niche: i64,
    pub max_per_niche: i64,
    pub min_sample: i64,
    pub exploit_weight: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ChannelConfig {
    /// Human-readable niche name, used in run logs and the production plan.
    pub name: String,
    /// Stable niche key — must match what selector.rs keys quotas on.
    pub niche: String,
    /// Format tag ("long" | "short"); selector keys on (niche, format).
    pub format: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThrottleConfig {
    /// Global ceiling on concurrent outbound requests across ALL stages.
    #[serde(default = "default_throttle_concurrency")]
    pub max_concurrency: usize,
    /// Default inter-request floor (ms) for hosts without an explicit policy.
    #[serde(default = "default_throttle_interval_ms")]
    pub default_interval_ms: u64,
    /// Optional explicit per-host floors: host string -> min interval ms.
    #[serde(default)]
    pub host_intervals_ms: std::collections::HashMap<String, u64>,
}

impl ThrottleConfig {
    pub fn default_interval(&self) -> Duration {
        Duration::from_millis(self.default_interval_ms)
    }
}

fn default_throttle_concurrency() -> usize { 4 }
fn default_throttle_interval_ms() -> u64 { 200 }

#[derive(Debug, Clone, Deserialize)]
pub struct AuthConfig {
    /// OAuth client id — typically ${YT_CLIENT_ID}.
    pub client_id: String,
    /// OAuth client secret — typically ${YT_CLIENT_SECRET}.
    pub client_secret: String,
    /// Long-lived refresh token — typically ${YT_REFRESH_TOKEN}.
    pub refresh_token: String,
    /// Token endpoint; defaults to Google's if omitted.
    #[serde(default = "default_token_url")]
    pub token_url: String,
}

fn default_token_url() -> String {
    "https://oauth2.googleapis.com/token".to_string()
}

// ============================================================
//  ${ENV_VAR} resolution
// ============================================================

/// Resolve every ${VAR} in the raw TOML against the process environment.
///
/// Done on the RAW STRING before parse so it works uniformly across every
/// field of every nested struct — no per-field plumbing. A missing or empty
/// var becomes a collected error; we report ALL of them at once rather than
/// failing one at a time across repeated runs.
fn resolve_env_vars(raw: &str) -> Result<String> {
    // Matches ${IDENT} where IDENT is the usual env-var charset.
    let re = Regex::new(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
        .expect("static env-var regex is valid");

    let mut missing: Vec<String> = Vec::new();

    let resolved = re.replace_all(raw, |caps: &regex::Captures| {
        let var = &caps[1];
        match std::env::var(var) {
            Ok(val) if !val.trim().is_empty() => val,
            _ => {
                // Record and leave a sentinel; we'll bail after the full pass.
                if !missing.contains(&var.to_string()) {
                    missing.push(var.to_string());
                }
                String::new()
            }
        }
    });

    if !missing.is_empty() {
        bail!(
            "missing/empty required environment variable(s): {}",
            missing.join(", ")
        );
    }

    Ok(resolved.into_owned())
}

// ============================================================
//  Load + validate
// ============================================================

impl AppConfig {
    /// Read TOML from disk, resolve env vars, parse, then validate. Any one of
    /// those failing returns a contextful error pointing at the actual cause.
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading config file {path}"))?;

        let resolved = resolve_env_vars(&raw)
            .with_context(|| format!("resolving env vars in {path}"))?;

        let cfg: AppConfig = toml::from_str(&resolved)
            .with_context(|| format!("parsing config {path}"))?;

        cfg.validate()
            .with_context(|| format!("validating config {path}"))?;

        Ok(cfg)
    }

    /// Sanity checks for the misconfigs that otherwise fail deep in a stage.
    /// Cheap, readable, and they save you a 20-minute log dive every time.
    fn validate(&self) -> Result<()> {
        if self.db_path.trim().is_empty() {
            bail!("db_path is empty");
        }

        if self.channel.niche.trim().is_empty() {
            bail!("channel.niche is empty — selector.rs keys quotas on it");
        }
        match self.channel.format.as_str() {
            "long" | "short" => {}
            other => bail!("channel.format must be 'long' or 'short', got '{other}'"),
        }

        if self.throttle.max_concurrency == 0 {
            bail!("throttle.max_concurrency must be >= 1 (0 deadlocks every stage)");
        }

        // upload.privacy_status is the classic footgun — a typo here silently
        // publishes everything PUBLIC or rejects the insert outright.
        match self.upload.privacy_status.as_str() {
            "public" | "unlisted" | "private" => {}
            other => bail!(
                "upload.privacy_status must be public|unlisted|private, got '{other}'"
            ),
        }

        // Auth fields are required and env-resolved; if a ${VAR} resolved to
        // empty it'd have failed in resolve_env_vars, but a literal empty in
        // the TOML slips through — catch it here.
        if self.auth.client_id.trim().is_empty()
            || self.auth.client_secret.trim().is_empty()
            || self.auth.refresh_token.trim().is_empty()
        {
            bail!("auth.client_id / client_secret / refresh_token must all be set");
        }

        Ok(())
    }

    /// Construct the shared Throttle straight from this config. Called once in
    /// pipeline.rs boot(), then cloned into every network stage.
    pub fn build_throttle(&self) -> crate::throttle::Throttle {
        let mut t = crate::throttle::Throttle::new(
            self.throttle.max_concurrency,
            self.throttle.default_interval(),
        );
        for (host, ms) in &self.throttle.host_intervals_ms {
            t = t.with_host_policy(host, Duration::from_millis(*ms));
        }
        t
    }
}

// ============================================================
//  Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_resolves_present_var() {
        std::env::set_var("CFG_TEST_PRESENT", "hello");
        let out = resolve_env_vars(r#"key = "${CFG_TEST_PRESENT}""#).unwrap();
        assert_eq!(out, r#"key = "hello""#);
    }

    #[test]
    fn env_missing_var_is_hard_error() {
        std::env::remove_var("CFG_TEST_ABSENT_XYZ");
        let err = resolve_env_vars(r#"k = "${CFG_TEST_ABSENT_XYZ}""#).unwrap_err();
        assert!(err.to_string().contains("CFG_TEST_ABSENT_XYZ"));
    }

    #[test]
    fn env_empty_var_counts_as_missing() {
        std::env::set_var("CFG_TEST_EMPTY", "   ");
        let err = resolve_env_vars(r#"k = "${CFG_TEST_EMPTY}""#).unwrap_err();
        assert!(err.to_string().contains("CFG_TEST_EMPTY"));
    }

    #[test]
    fn multiple_missing_reported_together() {
        std::env::remove_var("CFG_MISS_A");
        std::env::remove_var("CFG_MISS_B");
        let err = resolve_env_vars("a=${CFG_MISS_A}\nb=${CFG_MISS_B}").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("CFG_MISS_A") && msg.contains("CFG_MISS_B"));
    }
}