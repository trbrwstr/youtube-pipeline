# forgotten-classics-engine

A faceless-YouTube production pipeline in Rust. It turns public-domain books
into finished, uploaded videos — and measures which niches earn, then
reallocates its own production toward what pays.

The whole thing is one shared engine. Niches differ only by **config file** and
**visual template**; the code path is identical. Books are the proving ground
because the inputs are the friendliest to debug: clean UTF-8, public domain,
zero scraping ethics.

```
ingest → hook → render → upload → analytics → selector → (loop)
```

---

## Table of Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Database Schema](#database-schema)
- [Modules](#modules)
- [Running the Pipeline](#running-the-pipeline)
- [The Feedback Loop](#the-feedback-loop)
- [Operational Notes](#operational-notes)
- [Troubleshooting](#troubleshooting)
- [Roadmap](#roadmap)

---

## Architecture

Every stage reads from and writes back to a single SQLite database
(`books.db`). Stages are independent binaries that can run on their own
schedule; the DB is the only thing they share. This means any stage is
**resumable** — kill it mid-run, restart, and it picks up whatever didn't
finish via `LEFT JOIN ... IS NULL` style filters and the `pipeline_state`
table.

```
                ┌─────────────┐
                │ Project     │   pg_catalog.csv.gz (+ ETag cache)
                │ Gutenberg   │   per-book -0.txt with URL fallbacks
                └──────┬──────┘
                       │
              ┌────────▼────────┐
              │   ingest.rs     │  parse → filter → strip boilerplate → store
              └────────┬────────┘
                       │  books
              ┌────────▼────────┐
              │    hook.rs      │  deterministic baseline / LLM upgrade
              └────────┬────────┘
                       │  script_frames.hook
              ┌────────▼────────┐
              │   render.rs*    │  TTS + ffmpeg zoompan + subtitle burn
              └────────┬────────┘
                       │  output_path, thumb_path
              ┌────────▼────────┐
              │ thumbnail.rs +  │  upload video + set custom thumbnail
              │   upload.rs     │
              └────────┬────────┘
                       │  youtube_id, uploads
              ┌────────▼────────┐
              │  analytics.rs   │  per-video stats → video_stats (time series)
              └────────┬────────┘
                       │  niche/format ranking
              ┌────────▼────────┐
              │  selector.rs    │  reallocate quota → production_plan
              └────────┬────────┘
                       └──────────► feeds next ingest/hook batch limits
```

\* `render.rs` is the assembler that proves the engine: if `script.json →
final.mp4` works for a book, it works for every niche.

---

## Data Flow

| Stage | Reads | Writes | Key guarantee |
|-------|-------|--------|---------------|
| `ingest` | Gutenberg catalog + text | `books` | `INSERT OR IGNORE` on `gutenberg_id`, no double-inserts |
| `hook` | `books` (unscripted) | `script_frames.hook` | Deterministic fallback never errors |
| `render` | `script_frames` | `output_path`, `thumb_path` | Idempotent per `book_id` |
| `upload` | rendered files | `youtube_id`, `uploads` | OAuth via cached refresh token |
| `thumbnail` | `thumb_path` | `thumb_set` | Sets custom thumb post-upload |
| `analytics` | `uploads` | `video_stats` | One row per `(video_id, snapshot_date)` |
| `selector` | `video_stats` + `uploads` | `production_plan` | Pure allocator core, clamped quotas |

---

## Prerequisites

- **Rust** 1.75+ (`rustup` recommended)
- **ffmpeg** on `PATH` (zoompan, subtitle burn, concat)
- **A TTS provider** (local or API — one consistent voice profile)
- **Google Cloud project** with YouTube Data API v3 + YouTube Analytics API
  enabled, OAuth client credentials, and a channel in the
  YouTube Partner Program if you want revenue numbers
- **SQLite** is bundled via `rusqlite`'s `bundled` feature — no system install

---

## Installation

```bash
git clone <your-repo> forgotten-classics-engine
cd forgotten-classics-engine
cargo build --release
```

`Cargo.toml` dependency block:

```toml
[dependencies]
reqwest   = { version = "0.12", features = ["json", "stream"] }
tokio     = { version = "1", features = ["full"] }
rusqlite  = { version = "0.31", features = ["bundled"] }
serde     = { version = "1", features = ["derive"] }
serde_json = "1"
toml      = "0.8"
anyhow    = "1"
futures   = "0.3"
polars    = { version = "0.41", features = ["lazy", "csv"] }
chrono    = "0.4"
flate2    = "1"        # gzip catalog decompression
regex     = "1"        # boilerplate stripping
```

---

## Configuration

Config is per-niche TOML with `${ENV_VAR}` resolution **at load time**. One
file per channel; the engine reads the niche it's told to run.

`config/forgotten_classics.toml`:

```toml
[hook]
api_base      = "https://api.openai.com/v1"
api_key       = "${OPENAI_API_KEY}"     # resolved at HookConfig::load
model         = "gpt-4o-mini"
wpm           = 150.0                    # words-per-minute read estimate
max_hook_secs = 8.0                      # bail to deterministic if LLM exceeds
timeout_secs  = 20
concurrency   = 6                        # semaphore cap on parallel LLM calls
system_prompt = """
You are a hook writer for a Forgotten Classics YouTube channel. \
Tone: hushed, reverent, a little ominous. Make century-old prose feel \
urgent and alive. Never explain the book; tease it.
"""

[ingest]
catalog_url   = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz"
language      = "en"
max_year      = 1928                     # "Issued" used loosely (upload date)
throttle_ms   = 500                      # polite per-fetch spacing
text_fallback = ["-0.txt", "-8.txt", ".txt"]

[selector]
total_budget   = 100
min_per_niche  = 5
max_per_niche  = 60
min_sample     = 10
exploit_weight = 0.7                      # 0=explore evenly, 1=exploit winners
```

Required environment variables:

```bash
export OPENAI_API_KEY="sk-..."
export YT_CHANNEL_ID="UC..."
# OAuth client creds consumed by auth.rs (refresh-token cache)
```

> **Note on `${VAR}` resolution:** only `api_key` and `api_base` expand env
> vars by default (see `resolve_env`). A bare literal passes through
> untouched, so you can hardcode for local testing without breaking the
> loader.

---

## Database Schema

Single SQLite file, `books.db`. Pragmas set on every open:

```sql
PRAGMA journal_mode = WAL;       -- concurrent readers + one writer
PRAGMA synchronous  = NORMAL;    -- fast, safe enough under WAL
PRAGMA busy_timeout = 5000;      -- wait 5s instead of erroring on lock
PRAGMA foreign_keys = ON;        -- enforce referential integrity
```

### `books`

The catalog backlog. `gutenberg_id` carries a `UNIQUE` constraint so the
`INSERT OR IGNORE` in ingest can't double-insert; `id` autoincrements as the
internal join key.

```sql
CREATE TABLE IF NOT EXISTS books (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    gutenberg_id  INTEGER NOT NULL UNIQUE,
    title         TEXT NOT NULL,
    author        TEXT NOT NULL,        -- cleaned ("Last, First" → "First Last")
    year          INTEGER,
    body          TEXT,                 -- boilerplate-stripped full text
    created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
```

### `script_frames`

One generated video's worth of state, `UNIQUE(book_id)` so a re-run upserts
rather than duplicates. Carries the whole lifecycle from hook text through to
YouTube ID.

```sql
CREATE TABLE IF NOT EXISTS script_frames (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    book_id        INTEGER NOT NULL,
    hook_text      TEXT,                -- on-screen / TTS hook
    audio_path     TEXT,
    audio_secs     REAL,
    output_path    TEXT,                -- final rendered mp4
    thumb_path     TEXT,
    yt_title       TEXT,
    yt_description TEXT,
    yt_tags        TEXT,                -- comma-joined
    youtube_id     TEXT,               -- populated after upload
    thumb_set      INTEGER NOT NULL DEFAULT 0,  -- 0/1 bool
    duration_secs  REAL,
    created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    FOREIGN KEY (book_id) REFERENCES books(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_frames_book ON script_frames(book_id);
```

### `pipeline_state`

Per-stage job ledger. The `mark_running`-before-spawn / `mark_done`-after
pattern (handled in `state.rs`) lives here. `UNIQUE(stage, book_id)` means a
book can be in exactly one state per stage.

```sql
CREATE TABLE IF NOT EXISTS pipeline_state (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    stage       TEXT NOT NULL,          -- "ingest"|"hook"|"render"|"upload"|...
    book_id     INTEGER NOT NULL,
    status      TEXT NOT NULL,          -- "pending"|"running"|"done"|"failed"
    attempts    INTEGER NOT NULL DEFAULT 0,
    last_error  TEXT,
    updated_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    FOREIGN KEY (book_id) REFERENCES books(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_state_stage_book
    ON pipeline_state(stage, book_id);
```

### `video_stats` (analytics)

Time series — one row per `(video_id, snapshot_date)` so you track velocity,
not just a stale lifetime number.

```sql
CREATE TABLE IF NOT EXISTS video_stats (
    video_id            TEXT NOT NULL,
    snapshot_date       TEXT NOT NULL,
    views               INTEGER NOT NULL,
    est_minutes_watched INTEGER NOT NULL,
    avg_view_duration   REAL NOT NULL,
    avg_view_percentage REAL NOT NULL,
    likes               INTEGER NOT NULL,
    comments            INTEGER NOT NULL,
    subscribers_gained  INTEGER NOT NULL,
    est_revenue_usd     REAL NOT NULL,
    cpm_usd             REAL NOT NULL,
    PRIMARY KEY (video_id, snapshot_date)
);
CREATE INDEX IF NOT EXISTS idx_stats_video ON video_stats(video_id);
```

### `production_plan` (selector)

The actuator output. Keyed by `run_date` so each cycle is idempotent.

```sql
CREATE TABLE IF NOT EXISTS production_plan (
    run_date  TEXT NOT NULL,
    niche     TEXT NOT NULL,
    format    TEXT NOT NULL,
    quota     INTEGER NOT NULL,
    reason    TEXT NOT NULL,            -- human-readable why, for the run log
    PRIMARY KEY (run_date, niche, format)
);
```

### Migrations

Additive only. Each new column is attempted via `ALTER TABLE ... ADD COLUMN`
and the "duplicate column name" error is swallowed — so running migrate on an
already-current DB is a no-op, and old DBs upgrade in place without a version
table.

```rust
fn add_column(conn: &Connection, sql: &str) -> Result<()> {
    match conn.execute(sql, []) {
        Ok(_) => Ok(()),
        Err(e) if e.to_string().contains("duplicate column name") => Ok(()),
        Err(e) => Err(e.into()),
    }
}
```

---

## Modules

| File | Responsibility |
|------|----------------|
| `src/db.rs` | Schema, pragmas, additive migrations, connection open |
| `src/ingest.rs` | Catalog fetch (ETag cache), filter, boilerplate strip, store |
| `src/hook.rs` | Deterministic + LLM hook generation, semaphore batch runner |
| `src/state.rs` | `mark_running` / `mark_done` / `mark_failed` claim logic |
| `src/render.rs` | TTS + ffmpeg zoompan assembly → final.mp4 |
| `src/auth.rs` | OAuth, refresh-token-keyed access-token cache |
| `src/upload.rs` | Resumable YouTube Data API upload |
| `src/thumbnail.rs` | Custom thumbnail set post-upload |
| `src/analytics.rs` | Per-video stats pull + niche/format rollup |
| `src/selector.rs` | Pure allocator (explore/exploit) + plan persistence |

---

## Running the Pipeline

Each stage is a binary under `src/bin/`. Typical daily order:

```bash
# 1. Pull / refresh the catalog and fetch text for new candidates
cargo run --release --bin ingest

# 2. Generate hooks for unscripted books (LLM with deterministic fallback)
cargo run --release --bin hook

# 3. Render audio + video
cargo run --release --bin render

# 4. Upload + set thumbnails
cargo run --release --bin upload

# 5. Refresh stats (end window at now-2d for settled revenue)
YT_CHANNEL_ID=UC... cargo run --release --bin analytics

# 6. Reweight next run's production
cargo run --release --bin selector
```

Minimal `main` for the hook stage:

```rust
mod hook;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = hook::HookConfig::load("config/forgotten_classics.toml")?;
    let written = hook::run_batch("books.db", &cfg, 100).await?;
    println!("wrote {written} script frames");
    Ok(())
}
```

---

## The Feedback Loop

This is the point of the whole thing. Production isn't hand-tuned — it
self-selects.

1. **analytics** ranks niches by `revenue_per_video` (not RPM — RPM flatters
   Shorts; revenue-per-video folds in both rate and realistic volume).
2. **selector** blends an even split (exploration) with a score-weighted split
   (exploitation) via `exploit_weight`, clamps each into
   `[min_per_niche, max_per_niche]`, and forces under-`min_sample` niches onto
   the exploration track so noisy early numbers can't kill an unproven niche.
3. The plan lands in `production_plan`. Instead of a hardcoded
   `limit = 100`, the hook/ingest batch calls
   `quota_for(conn, today, "classics", "long")` to read its number.
4. Next cycle produces toward what pays. Repeat.

The one lever you touch over time is `exploit_weight`. Start `0.7`. If the
engine thrashes — dumping budget into a niche, watching it cool, yanking back —
ease toward `0.5`. If you trust your niches and just want to milk winners,
push toward `0.9`.

---

## Operational Notes

- **Write serialization:** a single `rusqlite` connection isn't `Sync` for
  writes. Under the batch runner, LLM calls run parallel under the semaphore;
  only the quick `store_frame` step serializes behind a `tokio::sync::Mutex`.
  WAL mode keeps readers unblocked during writes.
- **Analytics reporting lag:** revenue isn't final for ~2 days. The analytics
  binary ends its window at `now - 2 days` to avoid logging phantom `$0.00`s
  as truth.
- **Gutenberg `Issued` field:** it's the upload date, not original publication
  — treated loosely as a `max_year` filter, not ground truth.
- **Text URL fallback:** `-0.txt` isn't universal; the fetcher walks
  `text_fallback` patterns before giving up on a book.
- **Throttling:** ingest spaces fetches by `throttle_ms`; analytics paces
  sweeps at 250ms even though the quota is per-day, just to stay off the radar.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|--------------|-----|
| `env var OPENAI_API_KEY not set` | `${VAR}` unresolved at load | `export` it before running |
| `database is locked` | Concurrent writers, no WAL | Confirm `journal_mode=WAL` pragma ran |
| Hooks all look deterministic | Every LLM call failing | Check `api_base`, key, and `max_hook_secs` ceiling — long completions bail |
| Revenue all `$0.00` | YPP not active, or window too recent | Confirm monetization; end window at `now-2d` |
| Duplicate books | Missing `UNIQUE(gutenberg_id)` | Re-run migrate; `INSERT OR IGNORE` needs the constraint |
| Niche starved to zero | `min_per_niche` too low / score noise | Raise floor or `min_sample` |

---

## Roadmap

- `state.rs` — `mark_running`-before-spawn / `mark_done`-after claim logic so
  multiple workers can pull jobs without stepping on each other
- Shared semaphore across `ingest` + `hook` for one global rate limiter
- Velocity scoring in `analytics` (is a video still climbing, or dead?)
- Per-niche visual templates pluggable via config
- Multi-channel sweep loop in the analytics binary

---

## License

MIT. No warranties. Public-domain source material; respect Gutenberg's terms
of use for bulk access.
