// src/assemble.rs
use anyhow::{anyhow, Context, Result};
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

/// Probe audio duration via ffprobe so the video length matches the narration
/// exactly, regardless of what the ScriptFrame estimate said.
async fn probe_duration(audio: &Path) -> Result<f32> {
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