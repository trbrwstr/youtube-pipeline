// src/thumbnail.rs
//
// YouTube Data API v3 thumbnails.set, wired into the shared pipeline.
//
// Changes from the standalone version:
//   * Token minting comes from auth.rs (cached, 401-retry aware) — no local
//     fetch_access_token / resolve_env_var.
//   * Outbound calls go through the shared Throttle so thumbnail traffic
//     counts against the same global ceiling as upload (same googleapis host).
//   * Eligibility / running / done / failed all run through state.rs, so the
//     stage is resumable and a crash mid-batch leaves no half-finished flags.
//
// Quota: thumbnails.set costs 50 units (vs 1600 for insert). Cheap, but it
// draws from the SAME daily 10k pool as upload — budget them together.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use reqwest::{Client, StatusCode};
use rusqlite::{params, Connection};
use serde::Deserialize;

use crate::auth::{self, OAuthConfig};
use crate::config::AuthConfig;
use crate::throttle::Throttle;

/// Stable host key so thumbnail + upload share one googleapis pacing floor.
const HOST: &str = "googleapis.com";

// ============================================================
//  Config
// ============================================================

#[derive(Debug, Clone, Deserialize)]
pub struct ThumbnailConfig {
    /// Retries on transient failures (429 / 5xx / dropped connection).
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_timeout")]
    pub request_timeout_secs: u64,
}

fn default_max_retries() -> u32 { 3 }
fn default_timeout() -> u64 { 60 }

// ============================================================
//  Job + MIME
// ============================================================

#[derive(Debug, Clone)]
pub struct ThumbJob {
    pub book_id: i64,
    pub youtube_id: String,
    pub thumb_path: PathBuf,
}

/// YouTube accepts image/jpeg and image/png. Sniff by extension, default jpeg.
fn mime_for(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|e| e.to_str())
        .map(|s| s.to_ascii_lowercase())
        .as_deref()
    {
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        _ => "image/jpeg",
    }
}

// ============================================================
//  thumbnails.set — single binary upload, throttled + retried
// ============================================================

/// Set one custom thumbnail. The upload endpoint takes raw image bytes as the
/// body — no JSON wrapper, no multipart. On 401 we invalidate the cached token
/// and retry once; on 429/5xx we back off and retry up to max_retries.
async fn set_one(
    client: &Client,
    oauth: &OAuthConfig,
    throttle: &Throttle,
    cfg: &ThumbnailConfig,
    job: &ThumbJob,
) -> Result<()> {
    let bytes = tokio::fs::read(&job.thumb_path)
        .await
        .with_context(|| format!("reading thumbnail {}", job.thumb_path.display()))?;

    if bytes.is_empty() {
        bail!("thumbnail {} is empty", job.thumb_path.display());
    }
    // YouTube hard limit is 2 MB for custom thumbnails.
    if bytes.len() > 2 * 1024 * 1024 {
        bail!(
            "thumbnail {} is {} bytes (>2MB limit) — re-encode smaller",
            job.thumb_path.display(),
            bytes.len()
        );
    }

    let mime = mime_for(&job.thumb_path);
    let url = format!(
        "https://www.googleapis.com/upload/youtube/v3/thumbnails/set\
         ?uploadType=media&videoId={}",
        job.youtube_id
    );

    let mut attempt: u32 = 0;
    loop {
        // Pace against the shared global/host limiter. Guard held for the
        // whole send+receive, dropped before any backoff sleep below.
        let token = auth::fetch_access_token(client, oauth).await?;

        let result = {
            let _guard = throttle.acquire(HOST).await;

            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {token}"))?,
            );
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(mime));

            client
                .post(&url)
                .headers(headers)
                .body(bytes.clone())
                .send()
                .await
        };

        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                // Network-level failure (dropped conn, timeout) — retryable.
                if attempt < cfg.max_retries {
                    attempt += 1;
                    throttle
                        .backoff(attempt, std::time::Duration::from_millis(500),
                                 std::time::Duration::from_secs(30))
                        .await;
                    continue;
                }
                return Err(e).with_context(|| {
                    format!("thumbnails.set network error for video {}", job.youtube_id)
                });
            }
        };

        let status = resp.status();
        if status == StatusCode::OK {
            return Ok(());
        }

        let txt = resp.text().await.unwrap_or_default();

        // 401: token revoked server-side before our clock said it expired.
        // Evict the cached token and retry once without burning an attempt
        // budget on the first occurrence.
        if status == StatusCode::UNAUTHORIZED {
            auth::invalidate(oauth).await;
            if attempt < cfg.max_retries {
                attempt += 1;
                continue;
            }
            bail!("thumbnails.set 401 for video {} after retries: {txt}", job.youtube_id);
        }

        // 403: almost always the channel isn't phone-verified, so custom
        // thumbnails are disabled. NOT retryable — surface it loud.
        if status == StatusCode::FORBIDDEN {
            bail!(
                "thumbnails.set 403 for video {}: {} \
                 (most often: channel not phone-verified, so custom thumbnails \
                 are disabled — verify in YouTube Studio)",
                job.youtube_id,
                txt
            );
        }

        // 429 / 5xx: transient, back off and retry.
        if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
            if attempt < cfg.max_retries {
                attempt += 1;
                throttle
                    .backoff(attempt, std::time::Duration::from_millis(500),
                             std::time::Duration::from_secs(30))
                    .await;
                continue;
            }
            bail!(
                "thumbnails.set {status} for video {} after {} retries: {txt}",
                job.youtube_id, cfg.max_retries
            );
        }

        // Anything else (400, 404): not retryable, fail with the body.
        bail!("thumbnails.set returned {status} for video {}: {txt}", job.youtube_id);
    }
}

// ============================================================
//  SQLite glue — eligibility via state.rs, detail from books
// ============================================================

/// state.rs hands us the eligible book_ids for this stage; we hydrate the
/// youtube_id + thumb_path each one needs from the books table. A book is
/// only eligible if upload already stamped its youtube_id (state ordering
/// guarantees thumbnail runs after upload), and a thumb_path exists.
fn hydrate_job(conn: &Connection, book_id: i64) -> Result<Option<ThumbJob>> {
    let row = conn
        .query_row(
            "SELECT youtube_id, thumb_path
             FROM script_frames
             WHERE book_id = ?1",
            params![book_id],
            |row| {
                let yt: Option<String> = row.get(0)?;
                let tp: Option<String> = row.get(1)?;
                Ok((yt, tp))
            },
        )
        .optional()
        .with_context(|| format!("loading book {book_id} for thumbnail"))?;

    let (youtube_id, thumb_path) = match row {
        Some((Some(yt), Some(tp))) if !yt.is_empty() && !tp.is_empty() => (yt, tp),
        // Missing prerequisites — let the caller mark this failed/skipped.
        _ => return Ok(None),
    };

    Ok(Some(ThumbJob {
        book_id,
        youtube_id,
        thumb_path: PathBuf::from(thumb_path),
    }))
}

// rusqlite's .optional() lives on this trait.
use rusqlite::OptionalExtension;

// ============================================================
//  Stage entry point — per-item, state-gated by the runner loop
// ============================================================

/// Set the custom thumbnail for one already-uploaded book. Idempotent — a frame
/// whose `thumb_set` flag is already on is a no-op. A book missing its
/// youtube_id or thumb_path errors so it lands in the dead-letter queue.
pub async fn run_one(
    conn: &Connection,
    auth_cfg: &AuthConfig,
    cfg: &ThumbnailConfig,
    book_id: i64,
) -> Result<()> {
    let already_set: Option<i64> = conn
        .query_row(
            "SELECT thumb_set FROM script_frames WHERE book_id = ?1",
            params![book_id],
            |r| r.get(0),
        )
        .optional()
        .with_context(|| format!("checking thumb_set for book {book_id}"))?;
    if already_set == Some(1) {
        return Ok(());
    }

    let job = hydrate_job(conn, book_id)?
        .ok_or_else(|| anyhow::anyhow!("missing youtube_id or thumb_path at thumbnail stage"))?;

    let oauth = OAuthConfig::from_values(
        &auth_cfg.client_id,
        &auth_cfg.client_secret,
        &auth_cfg.refresh_token,
    )
    .context("building OAuthConfig for thumbnail stage")?;

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(cfg.request_timeout_secs))
        .build()?;
    let throttle = Throttle::from_default();

    set_one(&client, &oauth, &throttle, cfg, &job).await?;

    conn.execute(
        "UPDATE script_frames SET thumb_set = 1 WHERE book_id = ?1",
        params![book_id],
    )
    .with_context(|| format!("marking thumb_set for book {book_id}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn mime_png_detected() {
        assert_eq!(mime_for(Path::new("a/b/cover.PNG")), "image/png");
    }

    #[test]
    fn mime_jpeg_variants() {
        assert_eq!(mime_for(Path::new("x.jpg")), "image/jpeg");
        assert_eq!(mime_for(Path::new("x.jpeg")), "image/jpeg");
    }

    #[test]
    fn mime_unknown_defaults_jpeg() {
        assert_eq!(mime_for(Path::new("x.webp")), "image/jpeg");
        assert_eq!(mime_for(Path::new("noext")), "image/jpeg");
    }
}