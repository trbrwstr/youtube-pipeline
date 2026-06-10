// src/upload.rs
use crate::auth::{self, OAuthConfig};
use crate::throttle::Throttle;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, Deserialize)]
pub struct UploadConfig {
    pub privacy_status: String,   // "private" | "unlisted" | "public"
    pub category_id: String,      // "27" = Education, "24" = Entertainment
    pub made_for_kids: bool,
    pub chunk_bytes: usize,       // 8 MiB is a sane resumable chunk
    pub timeout_secs: u64,
}

#[derive(Serialize)]
struct VideoSnippet<'a> {
    title: &'a str,
    description: &'a str,
    tags: &'a [String],
    #[serde(rename = "categoryId")]
    category_id: &'a str,
}

#[derive(Serialize)]
struct VideoStatus<'a> {
    #[serde(rename = "privacyStatus")]
    privacy_status: &'a str,
    #[serde(rename = "selfDeclaredMadeForKids")]
    made_for_kids: bool,
}

#[derive(Serialize)]
struct VideoResource<'a> {
    snippet: VideoSnippet<'a>,
    status: VideoStatus<'a>,
}

#[derive(Deserialize)]
struct UploadResponse {
    id: String,
}

/// What the caller hands us — already-generated metadata plus the rendered file.
pub struct UploadJob<'a> {
    pub video_path: &'a Path,
    pub title: &'a str,
    pub description: &'a str,
    pub tags: &'a [String],
}

/// Step 1: open a resumable session. YouTube returns the upload URL in the
/// `Location` header — that URL is where the bytes go.
async fn initiate_session(
    client: &reqwest::Client,
    access_token: &str,
    cfg: &UploadConfig,
    job: &UploadJob<'_>,
    file_len: u64,
) -> Result<String> {
    let resource = VideoResource {
        snippet: VideoSnippet {
            title: job.title,
            description: job.description,
            tags: job.tags,
            category_id: &cfg.category_id,
        },
        status: VideoStatus {
            privacy_status: &cfg.privacy_status,
            made_for_kids: cfg.made_for_kids,
        },
    };

    let resp = client
        .post("https://www.googleapis.com/upload/youtube/v3/videos")
        .query(&[("uploadType", "resumable"), ("part", "snippet,status")])
        .bearer_auth(access_token)
        .header("X-Upload-Content-Type", "video/*")
        .header("X-Upload-Content-Length", file_len.to_string())
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .json(&resource)
        .send()
        .await
        .context("initiating resumable upload session")?
        .error_for_status()
        .context("non-2xx initiating upload")?;

    resp.headers()
        .get("location")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .ok_or_else(|| anyhow!("resumable init response had no Location header"))
}

/// Step 2: push the file in chunks. On a 308 the server tells us how far it
/// got via the Range header; we resume from there. On 200/201 it's done and
/// the body carries the video id.
async fn push_chunks(
    client: &reqwest::Client,
    session_url: &str,
    video_path: &Path,
    cfg: &UploadConfig,
    file_len: u64,
) -> Result<String> {
    let mut file = tokio::fs::File::open(video_path)
        .await
        .with_context(|| format!("opening {}", video_path.display()))?;

    let mut offset: u64 = 0;

    loop {
        let remaining = file_len - offset;
        let this_chunk = remaining.min(cfg.chunk_bytes as u64) as usize;
        let mut buf = vec![0u8; this_chunk];
        file.read_exact(&mut buf)
            .await
            .with_context(|| format!("reading chunk at offset {offset}"))?;

        let end = offset + this_chunk as u64 - 1;
        let content_range = format!("bytes {offset}-{end}/{file_len}");

        let resp = client
            .put(session_url)
            .header("Content-Length", this_chunk.to_string())
            .header("Content-Range", &content_range)
            .timeout(Duration::from_secs(cfg.timeout_secs))
            .body(buf)
            .send()
            .await
            .with_context(|| format!("uploading chunk {content_range}"))?;

        let status = resp.status().as_u16();
        match status {
            // 308 Resume Incomplete — read how far the server committed.
            308 => {
                let next = resp
                    .headers()
                    .get("range")
                    .and_then(|v| v.to_str().ok())
                    .and_then(|r| r.rsplit('-').next())
                    .and_then(|n| n.parse::<u64>().ok())
                    .map(|last| last + 1)
                    .unwrap_or(end + 1);

                if next <= offset {
                    return Err(anyhow!("server range did not advance (stuck at {next})"));
                }
                // Re-seek in case the server committed less than we sent.
                use tokio::io::AsyncSeekExt;
                file.seek(std::io::SeekFrom::Start(next))
                    .await
                    .context("re-seeking after 308")?;
                offset = next;
            }
            // 200 / 201 — upload complete, body has the resource with its id.
            200 | 201 => {
                let parsed: UploadResponse = resp
                    .json()
                    .await
                    .context("parsing final upload response")?;
                return Ok(parsed.id);
            }
            other => {
                let body = resp.text().await.unwrap_or_default();
                return Err(anyhow!("upload chunk failed with HTTP {other}: {body}"));
            }
        }
    }
}

/// Full upload: token -> session -> chunked push -> video id.
/// Idempotency lives one level up in run_upload; this just moves bytes.
pub async fn upload_video(
    client: &reqwest::Client,
    throttle: &Throttle,
    oauth: &OAuthConfig,
    cfg: &UploadConfig,
    job: &UploadJob<'_>,
) -> Result<String> {
    let file_len = tokio::fs::metadata(job.video_path)
        .await
        .with_context(|| format!("statting {}", job.video_path.display()))?
        .len();
    if file_len == 0 {
        return Err(anyhow!("refusing to upload empty file {}", job.video_path.display()));
    }

    let _guard = throttle.acquire().await;
    let access_token = auth::fetch_access_token(client, oauth)
        .await
        .context("fetching access token for upload")?;

    let session_url = initiate_session(client, &access_token, cfg, job, file_len).await?;
    push_chunks(client, &session_url, job.video_path, cfg, file_len).await
}