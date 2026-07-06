// src/lib.rs
//
// Crate root for the youtube-pipeline engine. This file does three jobs and
// nothing else:
//
//   1. Declare every stage module so both binaries (pipeline.rs, orchestrator.rs)
//      can `use youtube_pipeline::<module>`.
//   2. Re-export the handful of types that cross module boundaries — the shared
//      `Book` / `ScriptFrame` records, the `AppConfig`, the `Throttle` — so
//      callers reach them at the crate root instead of memorizing which file
//      each lives in.
//   3. Hold the two genuinely-shared domain structs (`Book`, `ScriptFrame`)
//      that don't belong to any single stage because half the pipeline reads
//      them and the other half writes them.
//
// Design rule: nothing in here does work. No I/O, no logic, no `fn main`-style
// orchestration. lib.rs is a table of contents. The moment it grows a function
// that *does* something, that function wants its own module.

// ============================================================
// Module declarations — order mirrors the pipeline stage chain
// ============================================================

// --- infrastructure: shared by every stage ---
pub mod auth;
pub mod config; // unified AppConfig + ${ENV_VAR} resolution
pub mod db; // SQLite connection, migrations, query helpers
pub mod state; // resumable state machine (eligible_for_stage, mark_*)
pub mod throttle; // shared global rate limiter // OAuth: token mint/refresh/cache for YT API stages

// --- stages: one module per step in the chain ---
pub mod assemble; // audio + drawtext -> vertical 1080x1920 mp4
pub mod hook; // book -> hook_text (deterministic + LLM)
pub mod ingest; // Gutenberg catalog -> books table
pub mod metadata; // yt_title / yt_description / yt_tags
pub mod thumbnail; // thumbnail render + thumbnails.set upload
pub mod tts; // hook/voice line -> hash-cached MP3
pub mod upload; // resumable video upload, writes youtube_id back

// --- feedback loop: measure performance, reallocate production ---
pub mod analytics; // per-video stats -> video_stats time series
pub mod selector; // revenue-weighted quota allocation -> production_plan

// --- ops: durable schedules + run history for the dashboard's scheduler ---
pub mod automation;

// --- shared bounded-concurrency stage runner used by both binaries ---
pub mod runner;

// ============================================================
// Re-exports — the crate's public surface
// ============================================================
//
// Callers say `youtube_pipeline::AppConfig`, not
// `youtube_pipeline::config::AppConfig`. Only the types that actually cross
// module/binary boundaries are hoisted here; stage-internal helpers stay
// private to their module. Keep this list short — every name here is a promise.

pub use config::{AppConfig, ChannelConfig};
pub use state::StageStatus;
pub use throttle::{Throttle, ThrottleGuard};

// ============================================================
// Shared domain types
// ============================================================
//
// These two structs are the only ones that don't belong to a single stage:
// `ingest` writes a Book, `hook`/`tts`/`metadata` read it; `hook` writes a
// ScriptFrame, everything downstream reads it. Putting them here — rather than
// in any one stage — keeps the dependency arrows pointing at the root instead
// of sideways between sibling stages.

use serde::{Deserialize, Serialize};

/// One public-domain work as ingested from Gutenberg. The internal `id` is the
/// foreign key every other table joins on; `gutenberg_id` is kept ONLY for
/// dedup on re-ingest (it's `UNIQUE` in the schema, the internal `id` is not
/// tied to it). Decoupling the two means a future non-Gutenberg source slots
/// in without the rest of the pipeline knowing or caring where a book came from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Book {
    /// Internal primary key. The FK every other table references.
    pub id: i64,
    /// Source dedup key — UNIQUE, but deliberately not the internal id.
    pub gutenberg_id: i64,
    pub title: String,
    /// Empty string when the catalog has no author (anthologies, anon works).
    pub author: String,
    /// Comma-joined Gutenberg subject headings; feeds tag generation.
    pub subjects: String,
    /// Resolved -0.txt (or fallback) URL the ingest stage fetched from.
    pub text_url: String,
    /// Boilerplate-stripped body. Large — only loaded when a stage needs it.
    pub body: String,
    /// Unix seconds when this row was first written.
    pub fetched_at: i64,
}

impl Book {
    /// True when we have enough to generate a video: a title and a non-trivial
    /// body. Authorless works are fine (handled downstream), an empty body is
    /// not — that's a failed fetch masquerading as a row.
    pub fn is_renderable(&self) -> bool {
        !self.title.trim().is_empty() && self.body.trim().len() > 200
    }

    /// Display label used in logs and the ops CLI. Falls back gracefully when
    /// the author is unknown so log lines never read "  by ".
    pub fn label(&self) -> String {
        if self.author.trim().is_empty() {
            self.title.clone()
        } else {
            format!("{} by {}", self.title, self.author)
        }
    }
}

/// The per-book production record — one row per book (`UNIQUE(book_id)`), the
/// "one book, one video" contract. Every stage after `hook` reads its inputs
/// from here and writes its outputs back: `hook` fills `hook_text`, `tts` fills
/// `audio_path`, `assemble` fills `output_path`, `metadata` fills the `yt_*`
/// columns, `upload` fills `youtube_id`. A fully-populated row is a published
/// video; a partial row tells you exactly how far the chain got.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScriptFrame {
    pub book_id: i64,

    /// The approved hook line — the spoken/displayed opener. Written by `hook`.
    pub hook_text: String,

    /// Content-addressed MP3 path under cache/audio/. Written by `tts`.
    pub audio_path: Option<String>,
    /// Spoken-line duration in seconds (ffprobe), used to time the drawtext.
    pub audio_secs: Option<f32>,

    /// Final assembled vertical mp4 under data/video/. Written by `assemble`.
    pub output_path: Option<String>,

    // --- metadata stage outputs ---
    pub yt_title: Option<String>,
    pub yt_description: Option<String>,
    /// Newline-joined tag list (re-split on read). Written by `metadata`.
    pub yt_tags: Option<String>,

    // --- thumbnail + upload outputs ---
    pub thumb_path: Option<String>,
    /// The published video id. Its presence is the idempotency guard that
    /// stops a re-run from double-publishing. Written by `upload`.
    pub youtube_id: Option<String>,
}

impl ScriptFrame {
    /// Tags come out of SQLite as one newline-joined blob; split back to a Vec
    /// on read so callers never parse the storage format themselves.
    pub fn tags(&self) -> Vec<String> {
        self.yt_tags
            .as_deref()
            .map(|blob| {
                blob.lines()
                    .map(str::trim)
                    .filter(|t| !t.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// True once `upload` has stamped a video id — the single source of truth
    /// for "already published, do not re-run upload."
    pub fn is_published(&self) -> bool {
        self.youtube_id
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false)
    }

    /// The stage chain reads this to decide whether `assemble` has everything
    /// it needs: audio rendered and a measured duration to time text against.
    pub fn ready_to_assemble(&self) -> bool {
        self.audio_path
            .as_ref()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
            && self.audio_secs.unwrap_or(0.0) > 0.0
    }
}

// ============================================================
// Crate-wide constants
// ============================================================
//
// The few magic strings that more than one module references. Keeping them
// here means a stage-name typo is a compile error, not a silently-never-matched
// row in pipeline_state.

/// Stage labels — must match exactly what `state.rs` stamps and what the
/// orchestrator's `Stage::label()` returns. Both sides import these so the
/// strings can never drift apart.
pub mod stages {
    pub const INGEST: &str = "ingest";
    pub const HOOK: &str = "hook";
    pub const TTS: &str = "tts";
    pub const ASSEMBLE: &str = "assemble";
    pub const METADATA: &str = "metadata";
    pub const THUMBNAIL: &str = "thumbnail";
    pub const UPLOAD: &str = "upload";

    /// The chain in execution order — upload precedes thumbnail because
    /// thumbnails.set needs the published video id. `db.rs` and the ops CLI
    /// iterate this for the status grid so adding a stage here surfaces it
    /// everywhere at once.
    pub const ALL: &[&str] = &[INGEST, HOOK, TTS, ASSEMBLE, METADATA, UPLOAD, THUMBNAIL];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renderable_requires_title_and_body() {
        let mut b = Book {
            id: 1,
            gutenberg_id: 1342,
            title: "Pride and Prejudice".into(),
            author: "Jane Austen".into(),
            subjects: "Fiction".into(),
            text_url: "https://www.gutenberg.org/files/1342/1342-0.txt".into(),
            body: "x".repeat(300),
            fetched_at: 0,
        };
        assert!(b.is_renderable());

        b.body = "too short".into();
        assert!(!b.is_renderable());

        b.body = "x".repeat(300);
        b.title = "   ".into();
        assert!(!b.is_renderable());
    }

    #[test]
    fn label_handles_missing_author() {
        let b = Book {
            id: 1,
            gutenberg_id: 1,
            title: "Anonymous Tract".into(),
            author: "".into(),
            subjects: String::new(),
            text_url: String::new(),
            body: String::new(),
            fetched_at: 0,
        };
        assert_eq!(b.label(), "Anonymous Tract");
    }

    #[test]
    fn tags_round_trip_from_blob() {
        let sf = ScriptFrame {
            yt_tags: Some("classic\n public domain \n\naudiobook\n".into()),
            ..Default::default()
        };
        assert_eq!(sf.tags(), vec!["classic", "public domain", "audiobook"]);
    }

    #[test]
    fn published_only_when_id_nonempty() {
        let mut sf = ScriptFrame::default();
        assert!(!sf.is_published());
        sf.youtube_id = Some("   ".into());
        assert!(!sf.is_published());
        sf.youtube_id = Some("dQw4w9WgXcQ".into());
        assert!(sf.is_published());
    }

    #[test]
    fn assemble_readiness_needs_audio_and_duration() {
        let mut sf = ScriptFrame {
            audio_path: Some("cache/audio/abc.mp3".into()),
            audio_secs: Some(6.4),
            ..Default::default()
        };
        assert!(sf.ready_to_assemble());

        sf.audio_secs = Some(0.0);
        assert!(!sf.ready_to_assemble());

        sf.audio_secs = Some(6.4);
        sf.audio_path = None;
        assert!(!sf.ready_to_assemble());
    }
}
