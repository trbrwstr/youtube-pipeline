// src/auth.rs
use anyhow::{anyhow, Context, Result};
use once_cell::sync::Lazy;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";

pub fn resolve_env_var(value: &str) -> Result<String> {
    if let Some(inner) = value.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        std::env::var(inner).with_context(|| format!("env var {inner} not set"))
    } else {
        Ok(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct OAuthConfig {
    pub client_id: String,
    pub client_secret: String,
    pub refresh_token: String,
}

impl OAuthConfig {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            client_id: resolve_env_var("${GOOGLE_CLIENT_ID}")?,
            client_secret: resolve_env_var("${GOOGLE_CLIENT_SECRET}")?,
            refresh_token: resolve_env_var("${YT_REFRESH_TOKEN}")?,
        })
    }

    pub fn from_values(client_id: &str, client_secret: &str, refresh_token: &str) -> Result<Self> {
        Ok(Self {
            client_id: resolve_env_var(client_id)?,
            client_secret: resolve_env_var(client_secret)?,
            refresh_token: resolve_env_var(refresh_token)?,
        })
    }

    fn cache_key(&self) -> &str {
        &self.refresh_token
    }
}

#[derive(Debug, Deserialize)]
struct RefreshResponse {
    access_token: String,
    expires_in: u64,
    token_type: String,
}

#[derive(Debug, Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

static TOKEN_CACHE: Lazy<Mutex<HashMap<String, CachedToken>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

const EXPIRY_SLACK: Duration = Duration::from_secs(60);

pub async fn fetch_access_token(client: &reqwest::Client, cfg: &OAuthConfig) -> Result<String> {
    let key = cfg.cache_key().to_string();

    {
        let cache = TOKEN_CACHE.lock().await;
        if let Some(tok) = cache.get(&key) {
            if tok.expires_at > Instant::now() + EXPIRY_SLACK {
                return Ok(tok.access_token.clone());
            }
        }
    }

    let params = [
        ("client_id", cfg.client_id.as_str()),
        ("client_secret", cfg.client_secret.as_str()),
        ("refresh_token", cfg.refresh_token.as_str()),
        ("grant_type", "refresh_token"),
    ];

    let resp = client
        .post(TOKEN_ENDPOINT)
        .form(&params)
        .send()
        .await
        .context("posting refresh-token grant")?;

    let status = resp.status();
    let text = resp.text().await?;

    if !status.is_success() {
        return Err(anyhow!(
            "token refresh failed ({status}): {text} \
             — if this is invalid_grant, re-run oauth_bootstrap and check the \
             consent screen isn't in Testing mode"
        ));
    }

    let parsed: RefreshResponse =
        serde_json::from_str(&text).with_context(|| format!("parsing refresh response: {text}"))?;

    if parsed.token_type != "Bearer" {
        return Err(anyhow!(
            "unexpected token_type {:?}, expected Bearer",
            parsed.token_type
        ));
    }

    let expires_at = Instant::now() + Duration::from_secs(parsed.expires_in);

    {
        let mut cache = TOKEN_CACHE.lock().await;
        cache.insert(
            key,
            CachedToken {
                access_token: parsed.access_token.clone(),
                expires_at,
            },
        );
    }

    Ok(parsed.access_token)
}

pub async fn invalidate(cfg: &OAuthConfig) {
    let mut cache = TOKEN_CACHE.lock().await;
    cache.remove(cfg.cache_key());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_passthrough_literal() {
        assert_eq!(resolve_env_var("plain-secret").unwrap(), "plain-secret");
    }

    #[test]
    fn resolve_reads_env() {
        std::env::set_var("AUTH_TEST_VAR", "from-env");
        assert_eq!(resolve_env_var("${AUTH_TEST_VAR}").unwrap(), "from-env");
    }

    #[test]
    fn resolve_missing_env_errors() {
        assert!(resolve_env_var("${DEFINITELY_NOT_SET_12345}").is_err());
    }
}
