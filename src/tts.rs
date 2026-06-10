// src/tts.rs
use crate::throttle::Throttle;
use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct TtsConfig {
    pub api_base: String,      // OpenAI-compatible TTS endpoint
    pub api_key: String,
    pub model: String,         // e.g. "tts-1" or a local model name
    pub voice: String,         // "onyx", "echo", etc.
    pub format: String,        // "mp3"
    pub speed: f32,            // 0.25..4.0
    pub cache_dir: String,     // where rendered audio lives
    pub timeout_secs: u64,
}

#[derive(Serialize)]
struct TtsRequest<'a> {
    model: &'a str,
    voice: &'a str,
    input: &'a str,
    response_format: &'a str,
    speed: f32,
}

/// Result of a synth: the path on disk plus whether we paid for it this run.
pub struct TtsOutput {
    pub path: PathBuf,
    pub cached: bool,
}

/// Hash voice + model + speed + text so any parameter change busts the cache,
/// but identical re-runs reuse bytes already on disk.
fn cache_key(cfg: &TtsConfig, text: &str) -> String {
    let mut h = Sha256::new();
    h.update(cfg.model.as_bytes());
    h.update(b"\0");
    h.update(cfg.voice.as_bytes());
    h.update(b"\0");
    h.update(cfg.speed.to_le_bytes());
    h.update(b"\0");
    h.update(text.as_bytes());
    let digest = h.finalize();
    hex::encode(&digest[..16]) // 32 hex chars is plenty
}

/// Synthesize `text` to an audio file. Idempotent: if the cache hit exists
/// and is non-empty, returns it without touching the network.
pub async fn synthesize(
    client: &reqwest::Client,
    throttle: &Throttle,
    cfg: &TtsConfig,
    text: &str,
) -> Result<TtsOutput> {
    let key = cache_key(cfg, text);
    let dir = Path::new(&cfg.cache_dir);
    tokio::fs::create_dir_all(dir)
        .await
        .with_context(|| format!("creating tts cache dir {}", dir.display()))?;

    let out_path = dir.join(format!("{key}.{}", cfg.format));

    // Cache hit: non-empty file already rendered. Skip the network entirely.
    if let Ok(meta) = tokio::fs::metadata(&out_path).await {
        if meta.len() > 0 {
            return Ok(TtsOutput { path: out_path, cached: true });
        }
    }

    let _guard = throttle.acquire().await;

    let req = TtsRequest {
        model: &cfg.model,
        voice: &cfg.voice,
        input: text,
        response_format: &cfg.format,
        speed: cfg.speed,
    };

    let bytes = client
        .post(format!("{}/audio/speech", cfg.api_base.trim_end_matches('/')))
        .bearer_auth(&cfg.api_key)
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .json(&req)
        .send()
        .await
        .context("sending tts request")?
        .error_for_status()
        .context("non-2xx from tts endpoint")?
        .bytes()
        .await
        .context("reading tts audio body")?;

    if bytes.is_empty() {
        return Err(anyhow!("tts endpoint returned empty audio for key {key}"));
    }

    // Write to a temp sibling then rename — a crash mid-write never leaves a
    // truncated file that looks like a valid cache hit on the next run.
    let tmp = out_path.with_extension(format!("{}.partial", cfg.format));
    tokio::fs::write(&tmp, &bytes)
        .await
        .with_context(|| format!("writing temp audio {}", tmp.display()))?;
    tokio::fs::rename(&tmp, &out_path)
        .await
        .with_context(|| format!("promoting {} -> {}", tmp.display(), out_path.display()))?;

    Ok(TtsOutput { path: out_path, cached: false })
}