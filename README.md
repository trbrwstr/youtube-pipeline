# forgotten-classics-engine

A faceless-YouTube production pipeline in Rust. It turns public-domain books
into finished, uploaded videos — and measures which niches earn, then
reallocates its own production toward what pays.

The whole thing is one shared engine. Niches differ only by **config file** and
**visual template**; the code path is identical. Books are the proving ground
because the inputs are the friendliest to debug: clean UTF-8, public domain,
zero scraping ethics.

```
ingest → hook → tts → assemble → metadata → thumbnail → upload
                                                            │
                              (loop) ← selector ← analytics ←
```

---

> **Running it for real (esp. across multiple channels)?** See
> [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for per-channel OAuth, the daily
> produce → analytics → selector loop, and the cross-niche budget allocation.

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

Every stage reads from and writes back to one SQLite database **per niche**
(`db_path` in the config); the DB is the only thing the stages share. The
`pipeline` and `orchestrator` binaries walk the stage chain — you can run the
whole chain, a subset (`--stages`), or one niche vs. all of them. Any stage is
**resumable**: kill it mid-run, restart, and it picks up whatever didn't finish
via the `pipeline_state` ledger and each stage's idempotency guard.

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
                       │  script_frames.hook_text
              ┌────────▼────────┐
              │  tts.rs +       │  hash-cached narration + ffmpeg drawtext
              │  assemble.rs*   │  → vertical 1080x1920 mp4
              └────────┬────────┘
                       │  audio_path, output_path, thumb_path
              ┌────────▼────────┐
              │  metadata.rs    │  yt_title / yt_description / yt_tags
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │ upload.rs +     │  upload video + set custom thumbnail
              │ thumbnail.rs    │
              └────────┬────────┘
                       │  youtube_id, thumb_set
              ┌────────▼────────┐
              │  analytics.rs   │  per-video stats → video_stats (time series)
              └────────┬────────┘
                       │  niche/format ranking
              ┌────────▼────────┐
              │  selector.rs    │  reallocate quota → production_plan
              └────────┬────────┘
                       └──────────► feeds next ingest/hook batch limits
```

\* `assemble.rs` is the renderer that proves the engine: if `audio + still →
final.mp4` works for a book, it works for every niche. (`tts.rs` synthesizes
the narration; `assemble.rs` burns the caption and encodes.)

---

## Data Flow

| Stage | Reads | Writes | Key guarantee |
|-------|-------|--------|---------------|
| `ingest` | Gutenberg catalog + text | `books` | `INSERT OR IGNORE` on `gutenberg_id`, no double-inserts |
| `hook` | `books` (unscripted) | `script_frames.hook_text` | Deterministic fallback never errors |
| `tts` | `script_frames.hook_text` | `audio_path`, `audio_secs` | Content-hash cached; skips the network on a hit |
| `assemble` | audio + background still | `output_path`, `thumb_path` | Idempotent per `book_id` (existing mp4 short-circuits) |
| `metadata` | `script_frames` + `books` | `yt_title/description/tags` | Deterministic fallback never errors |
| `upload` | rendered files | `youtube_id` | OAuth via cached refresh token; `youtube_id` is the publish guard |
| `thumbnail` | `youtube_id`, `thumb_path` | `thumb_set` | Sets custom thumb post-upload |
| `analytics` | `script_frames` (published) | `video_stats` | One row per `(video_id, snapshot_date)` |
| `selector` | `video_stats` + `channel_meta` | `production_plan` | Pure allocator core, clamped quotas |

Every per-item stage above runs through one shared bounded-concurrency runner
(`runner.rs`): items are claimed via `pipeline_state`, processed in parallel up
to `throttle.max_concurrency`, and re-runnable — each stage's idempotency guard
makes a repeat a no-op. `ingest` is the one batch stage (no per-book id exists
before it runs).

---

## Prerequisites

- **Rust** 1.75+ (`rustup` recommended)
- **ffmpeg** + **ffprobe** on `PATH` (drawtext caption burn, still-image encode,
  duration probe, thumbnail-frame extract)
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

This builds five binaries: `pipeline` (single-niche run + ops CLI),
`orchestrator` (multi-niche driver), `analytics`, `selector`, and
`oauth_bootstrap`. The dependency set lives in `Cargo.toml`:

```toml
[dependencies]
anyhow     = "1"
reqwest    = { version = "0.12", features = ["json", "stream"] }
tokio      = { version = "1", features = ["full"] }
rusqlite   = { version = "0.31", features = ["bundled"] }  # bundled SQLite, no system install
serde      = { version = "1", features = ["derive"] }
serde_json = "1"
toml       = "0.8"
futures    = "0.3"
flate2     = "1"        # gzip catalog decompression
regex      = "1"        # ${ENV_VAR} resolution
csv        = "1"        # catalog parsing
sha2       = "0.10"     # tts content-hash cache key
hex        = "0.4"
once_cell  = "1"        # OAuth token cache
clap       = { version = "4", features = ["derive", "env"] }
chrono     = { version = "0.4", features = ["clock"] }  # analytics/selector dates
```

---

## Configuration

Config is per-niche TOML with `${ENV_VAR}` resolution **at load time**. One
file per channel; the engine reads the niche it's told to run.

`config/forgotten_classics.toml`:

```toml
[hook]
api_base      = "https://api.openai.com/v1"
api_key       = "${OPENAI_API_KEY}"     # resolved at AppConfig::load
model         = "gpt-4o-mini"
wpm           = 150.0                    # words-per-minute read estimate
max_hook_secs = 8.0                      # bail to deterministic if LLM exceeds
timeout_secs  = 20
concurrency   = 6                        # legacy field; runner parallelism is [throttle].max_concurrency
system_prompt = """
You are a hook writer for a Forgotten Classics YouTube channel. \
Tone: hushed, reverent, a little ominous. Make century-old prose feel \
urgent and alive. Never explain the book; tease it.
"""

[ingest]
language        = "en"
max_issued_year = 1928                    # "Issued" used loosely (upload date)
cache_dir       = "cache"                 # catalog gzip + ETag cache live here
fetch_text      = true                    # hydrate full text during ingest

[selector]
total_budget   = 100
min_per_niche  = 5
max_per_niche  = 60
min_sample     = 10
exploit_weight = 0.7                      # 0=explore evenly, 1=exploit winners
```

A niche config is a full `AppConfig`: it also needs `db_path`, `max_attempts`,
and `[channel] [throttle] [auth] [tts] [assemble] [metadata] [thumbnail]
[upload]` blocks. See the working files in `config/` and the per-niche field
reference in [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

Required environment variables. The OAuth **app** (client id/secret) is shared,
but the **refresh token is per channel** — mint one per channel with
`oauth_bootstrap` and export it under the variable each niche config names:

```bash
export OPENAI_API_KEY="sk-..."
export YT_CLIENT_ID="...apps.googleusercontent.com"
export YT_CLIENT_SECRET="..."
export YT_REFRESH_TOKEN_CLASSICS="1//0g..."        # per-niche, see config/*.toml
```

> **Note on `${VAR}` resolution:** every `${VAR}` anywhere in the TOML is
> expanded against the environment at `AppConfig::load`, before parsing (see
> `config::resolve_env_vars`). A bare literal passes through untouched; a
> missing or empty *required* var is a hard error at load, not a silent failure
> deep in a stage. All missing vars are reported at once.

---

## Database Schema

One SQLite file per niche (the config's `db_path`). Pragmas set on every open:

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
    title         TEXT    NOT NULL,
    author        TEXT    NOT NULL DEFAULT 'Unknown', -- cleaned ("Last, First" → "First Last")
    language      TEXT    NOT NULL DEFAULT 'en',
    issued_year   INTEGER,                            -- PG "Issued" year (coarse age gate)
    subjects      TEXT    NOT NULL DEFAULT '',        -- comma-joined; feeds tag generation
    text_url      TEXT,                               -- resolved -0.txt (or fallback) URL
    body          TEXT,                               -- boilerplate-stripped full text
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
    youtube_id     TEXT,               -- populated after upload (the publish guard)
    thumb_set      INTEGER NOT NULL DEFAULT 0,  -- 0/1 bool
    created_at     INTEGER NOT NULL DEFAULT (strftime('%s','now')),
    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
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
    stage       TEXT NOT NULL,          -- "hook"|"tts"|"assemble"|"metadata"|"thumbnail"|"upload"
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

### `channel_meta`

A single row stamping which niche/format this DB belongs to. Each niche has its
own SQLite file, so one row is enough to attribute every video here to a
`(niche, format)` — `analytics` and `selector` join against it instead of
threading channel config through every stage.

```sql
CREATE TABLE IF NOT EXISTS channel_meta (
    id     INTEGER PRIMARY KEY CHECK (id = 1),  -- enforce exactly one row
    niche  TEXT NOT NULL,
    format TEXT NOT NULL
);
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
| `src/lib.rs` | Crate root: module decls + shared `Book` / `ScriptFrame` types |
| `src/config.rs` | Unified `AppConfig`, `${ENV_VAR}` resolution, validation |
| `src/db.rs` | Schema, pragmas, additive migrations, connection open |
| `src/state.rs` | `eligible_for_stage` + `mark_running/done/failed` claim logic |
| `src/throttle.rs` | Shared global rate limiter (concurrency cap + per-host floors) |
| `src/runner.rs` | Bounded-concurrency stage runner shared by both binaries |
| `src/ingest.rs` | Catalog fetch (ETag cache), filter, boilerplate strip, store |
| `src/hook.rs` | Deterministic + LLM hook generation, batch-wide cost guard |
| `src/tts.rs` | Hook line → hash-cached narration audio |
| `src/assemble.rs` | ffmpeg drawtext assembly → vertical mp4 (+ thumb frame) |
| `src/metadata.rs` | `yt_title` / `yt_description` / `yt_tags` (LLM + fallback) |
| `src/auth.rs` | OAuth, refresh-token-keyed access-token cache |
| `src/upload.rs` | Resumable YouTube Data API upload |
| `src/thumbnail.rs` | Custom thumbnail set post-upload |
| `src/analytics.rs` | Per-video stats pull (one batched report) → `video_stats` |
| `src/selector.rs` | Pure allocator (explore/exploit) + plan persistence |

---

## Running the Pipeline

The stages are *not* separate binaries — `pipeline` walks the whole chain (or a
subset) for one niche, and `orchestrator` does it across every niche. Typical
daily loop:

```bash
# 1. PRODUCE — walk the chain for one niche (or a subset of stages).
cargo run --release --bin pipeline -- run --config config/forgotten_classics.toml
cargo run --release --bin pipeline -- run --config config/forgotten_classics.toml --stages hook,tts
cargo run --release --bin pipeline -- run --config config/forgotten_classics.toml --no-upload

#    ...or every niche at once, with a global concurrency cap:
cargo run --release --bin orchestrator -- --config-dir config --max-parallel 2

# 2. MEASURE — pull a fresh stats snapshot per channel into video_stats.
cargo run --release --bin analytics -- --config config/forgotten_classics.toml

# 3. REALLOCATE — one shared budget across all niches (cross-channel).
cargo run --release --bin selector -- --config-dir config
```

Operational subcommands on `pipeline` (all take `--config`):

```bash
pipeline status   # per-stage pending/running/done/failed grid
pipeline reap     --stale-secs 900    # reset crashed 'running' rows
pipeline retry    --stage upload      # requeue failed rows for a stage
pipeline dead                         # list dead-letter rows (exhausted retries)
```

`oauth_bootstrap` mints the per-channel refresh token (run once per channel).
The full end-to-end setup — OAuth, ffmpeg, assets, the daily loop — is in
[`docs/RUNBOOK.md`](docs/RUNBOOK.md).

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

- **Concurrency model:** `rusqlite::Connection` is `Send` but not `Sync`, so the
  runner gives each worker its own connection rather than sharing one across
  awaits. Network I/O overlaps up to `throttle.max_concurrency`; SQLite
  serializes the (short) writes under WAL + `busy_timeout`. One `reqwest::Client`
  and one `Throttle` are built per stage run and cloned into every worker.
- **Analytics reporting lag:** revenue isn't final for ~2 days. The analytics
  binary ends its window at `now - 2 days` to avoid logging phantom `$0.00`s
  as truth.
- **Gutenberg `Issued` field:** it's the upload date, not original publication
  — treated loosely as a `max_year` filter, not ground truth.
- **Text URL fallback:** `-0.txt` isn't universal; the fetcher walks a fixed
  list of patterns (`-0.txt`, `-8.txt`, `.txt`, `/cache/epub/pgN.txt`) before
  giving up on a book.
- **Throttling:** the shared `Throttle` caps global concurrency and enforces
  per-host floors from `[throttle].host_intervals_ms` (e.g. 250ms to
  `gutenberg.org`); analytics paces its sweep at 250ms even though the quota is
  per-day, just to stay off the radar.

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

Shipped:

- ✅ Resumable claim logic (`state.rs`: `eligible_for_stage` + `mark_*`)
- ✅ One global rate limiter shared across stages (`throttle.rs`)
- ✅ Bounded-concurrency runner (`runner.rs`) with shared client + throttle
- ✅ Per-niche visual templates via `[assemble]` config
- ✅ Multi-channel orchestration + cross-niche federated `selector`

Still open:

- Velocity scoring in `analytics` (is a video still climbing, or dead?)
- Live end-to-end validation of the network/ffmpeg stages (needs real OAuth,
  a TTS endpoint, ffmpeg, and a monetized channel)
- Structured logging / per-channel throughput + spend metrics
- Retry/backoff on the `tts` call (today a transient failure waits for the
  next run)

---

## License

MIT. No warranties. Public-domain source material; respect Gutenberg's terms
of use for bulk access.
