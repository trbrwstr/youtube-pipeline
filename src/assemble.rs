// src/assemble.rs
use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;

#[derive(Debug, Clone)]
pub struct AssembleInput {
    pub audio: PathBuf,        // from tts::synthesize
    pub background: PathBuf,    // still image, ideally 1080x1920
    pub out: PathBuf,           // final mp4
    pub duration_secs: f32,     // from ScriptFrame
}

/// `[assemble]` — the per-niche render settings that aren't per-book.
#[derive(Debug, Clone, Deserialize)]
pub struct AssembleConfig {
    /// Still image looped behind the narration (ideally 1080x1920).
    pub background_image: String,
    /// Directory the rendered mp4 + extracted thumbnail land in.
    #[serde(default = "default_out_dir")]
    pub out_dir: String,
}

fn default_out_dir() -> String { "data/video".to_string() }

/// Probe audio duration via ffprobe so the video length matches the narration
/// exactly, regardless of what the ScriptFrame estimate said.
pub(crate) async fn probe_duration(audio: &Path) -> Result<f32> {
    let output = Command::new("ffprobe")
        .args([
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
        ])
        .arg(audio)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("spawning ffprobe")?;

    if !output.status.success() {
        return Err(anyhow!(
            "ffprobe failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let s = String::from_utf8_lossy(&output.stdout);
    s.trim()
        .parse::<f32>()
        .with_context(|| format!("parsing ffprobe duration '{}'", s.trim()))
}

/// Build the final short: loop the still for the audio's length, overlay a
/// burned-in caption, mux the narration, encode to vertical H.264.
pub async fn assemble_short(input: &AssembleInput, caption: &str) -> Result<PathBuf> {
    if let Some(parent) = input.out.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("creating output dir {}", parent.display()))?;
    }

    let dur = probe_duration(&input.audio).await.unwrap_or(input.duration_secs);

    // Escape the caption for ffmpeg's drawtext: colons, single quotes, and
    // backslashes all bite you here.
    let safe = caption
        .replace('\\', "\\\\")
        .replace(':', "\\:")
        .replace('\'', "\u{2019}"); // swap straight apostrophe for a curly one

    let vf = format!(
        "scale=1080:1920:force_original_aspect_ratio=increase,\
         crop=1080:1920,\
         drawtext=text='{safe}':fontcolor=white:fontsize=58:\
         box=1:boxcolor=black@0.55:boxborderw=24:\
         x=(w-text_w)/2:y=h-text_h-220:line_spacing=12"
    );

    let status = Command::new("ffmpeg")
        .args(["-y", "-loop", "1"])
        .arg("-i").arg(&input.background)
        .arg("-i").arg(&input.audio)
        .args([
            "-t", &format!("{dur:.2}"),
            "-vf", &vf,
            "-c:v", "libx264",
            "-tune", "stillimage",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "192k",
            "-shortest",
        ])
        .arg(&input.out)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .status()
        .await
        .context("spawning ffmpeg")?;

    if !status.success() {
        return Err(anyhow!("ffmpeg exited with status {status}"));
    }

    Ok(input.out.clone())
}

/// Extract a single frame from the rendered video for use as a thumbnail.
/// Best-effort: returns the path only if ffmpeg actually produced a file.
async fn extract_thumb(video: &Path, out: &Path) -> Result<PathBuf> {
    let status = Command::new("ffmpeg")
        .args(["-y", "-ss", "1"])
        .arg("-i").arg(video)
        .args(["-frames:v", "1", "-q:v", "3"])
        .arg(out)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await
        .context("spawning ffmpeg for thumbnail")?;
    if status.success() && tokio::fs::metadata(out).await.map(|m| m.len() > 0).unwrap_or(false) {
        Ok(out.to_path_buf())
    } else {
        Err(anyhow!("thumbnail extraction produced no file"))
    }
}

/// Stage entry point: render this book's narration + caption into a vertical
/// mp4 and stamp `output_path` (and a best-effort `thumb_path`). Idempotent —
/// an existing rendered file short-circuits.
pub async fn run_one(conn: &Connection, cfg: &AssembleConfig, book_id: i64) -> Result<()> {
    let row: Option<(Option<String>, Option<String>, Option<f32>, Option<String>)> = conn
        .query_row(
            "SELECT hook_text, audio_path, audio_secs, output_path \
             FROM script_frames WHERE book_id = ?1",
            params![book_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .with_context(|| format!("loading frame for assemble book {book_id}"))?;

    let (hook_text, audio_path, audio_secs, output_path) = match row {
        Some(t) => t,
        None => anyhow::bail!("book {book_id} has no script frame for assemble"),
    };

    // Idempotency: already rendered and the file is still on disk.
    if let Some(p) = output_path {
        if !p.is_empty() && tokio::fs::metadata(&p).await.map(|m| m.len() > 0).unwrap_or(false) {
            return Ok(());
        }
    }

    let audio = audio_path
        .filter(|p| !p.is_empty())
        .ok_or_else(|| anyhow!("book {book_id} has no audio for assemble"))?;
    let caption = hook_text.unwrap_or_default();

    tokio::fs::create_dir_all(&cfg.out_dir)
        .await
        .with_context(|| format!("creating out_dir {}", cfg.out_dir))?;

    let out = PathBuf::from(&cfg.out_dir).join(format!("{book_id}.mp4"));
    let input = AssembleInput {
        audio: PathBuf::from(&audio),
        background: PathBuf::from(&cfg.background_image),
        out: out.clone(),
        duration_secs: audio_secs.unwrap_or(0.0),
    };
    assemble_short(&input, &caption).await?;

    // Best-effort thumbnail; absence is not fatal (thumbnail stage will skip).
    let thumb = PathBuf::from(&cfg.out_dir).join(format!("{book_id}.jpg"));
    let thumb_path: Option<String> = extract_thumb(&out, &thumb)
        .await
        .ok()
        .map(|p| p.to_string_lossy().into_owned());

    conn.execute(
        "UPDATE script_frames SET output_path = ?1, thumb_path = COALESCE(?2, thumb_path) \
         WHERE book_id = ?3",
        params![out.to_string_lossy(), thumb_path, book_id],
    )
    .with_context(|| format!("recording output for book {book_id}"))?;
    Ok(())
}