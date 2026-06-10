// src/db.rs
//
// Schema + migrations for the whole pipeline. One init() that every stage
// calls at startup. Idempotent: CREATE TABLE IF NOT EXISTS plus tolerant
// ALTER TABLE for columns added after a table already existed in the wild.
//
// Table map:
//   books          - one row per Gutenberg work (ingest writes, everyone reads)
//   script_frames  - one row per video plan (hook/tts/assemble write, upload reads)
//   pipeline_state - per-(stage, book) job state for crash-resumable batches
//
// Cargo.toml deps (workspace set):
// rusqlite = { version = "0.31", features = ["bundled"] }
// anyhow   = "1"

use anyhow::{Context, Result};
use rusqlite::Connection;

/// Open the DB at `path`, apply pragmas, run every migration. Call this once
/// per process before any stage touches the connection.
pub fn open_and_init(path: &str) -> Result<Connection> {
    // Create the parent directory so a fresh checkout (where ./data/ doesn't
    // exist yet) can open the DB instead of erroring with SQLITE_CANTOPEN.
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating db dir {}", parent.display()))?;
        }
    }
    let conn = Connection::open(path).with_context(|| format!("opening db {path}"))?;
    apply_pragmas(&conn)?;
    init(&conn)?;
    Ok(conn)
}

/// WAL gives you concurrent readers alongside a writer — useful when one stage
/// reads while a long batch in another process writes. busy_timeout keeps a
/// brief lock contention from instantly erroring out as SQLITE_BUSY.
fn apply_pragmas(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;
         PRAGMA foreign_keys = ON;",
    )
    .context("applying pragmas")?;
    Ok(())
}

/// Create every table if absent, then run the additive ALTERs that bring an
/// older DB up to the current column set. Safe to run on every startup.
pub fn init(conn: &Connection) -> Result<()> {
    create_books(conn)?;
    create_script_frames(conn)?;
    create_pipeline_state(conn)?;
    create_channel_meta(conn)?;
    create_video_stats(conn)?;
    create_production_plan(conn)?;
    migrate_additive(conn)?;
    Ok(())
}

/// Stamp this DB's owning niche/format. Each niche has its own SQLite file, so
/// a single row is enough to attribute every video in here to a (niche, format)
/// — analytics joins against it instead of threading channel config through
/// every stage. Idempotent upsert; the binaries call it right after open.
pub fn set_channel_meta(conn: &Connection, niche: &str, format: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO channel_meta (id, niche, format) VALUES (1, ?1, ?2)
         ON CONFLICT(id) DO UPDATE SET niche = excluded.niche, format = excluded.format",
        rusqlite::params![niche, format],
    )
    .context("setting channel_meta")?;
    Ok(())
}

// ============================================================
// books — the catalog rows ingest produces
// ============================================================

fn create_books(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS books (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            gutenberg_id  INTEGER NOT NULL UNIQUE,
            title         TEXT    NOT NULL,
            author        TEXT    NOT NULL DEFAULT 'Unknown',
            language      TEXT    NOT NULL DEFAULT 'en',
            issued_year   INTEGER,
            subjects      TEXT    NOT NULL DEFAULT '',
            text_url      TEXT,
            body          TEXT,
            created_at    INTEGER NOT NULL DEFAULT (strftime('%s','now'))
        );

        -- ingest dedupes on gutenberg_id via INSERT OR IGNORE; the UNIQUE
        -- index above is what makes that work. This explicit index is for
        -- the book_exists() COUNT lookup hot path.
        CREATE INDEX IF NOT EXISTS idx_books_gutenberg ON books(gutenberg_id);",
    )
    .context("creating books table")?;
    Ok(())
}

// ============================================================
// script_frames — one planned video per row
// ============================================================
//
// hook stage:     writes hook_text
// tts stage:      writes audio_path, audio_secs
// assemble stage: writes output_path (the rendered mp4)
// metadata stage: writes yt_title, yt_description, yt_tags
// upload stage:   writes youtube_id
// thumbnail:      writes thumb_path (assemble) then thumb_set (thumbnail)

fn create_script_frames(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS script_frames (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            book_id         INTEGER NOT NULL,

            -- hook stage
            hook_text       TEXT,

            -- tts stage
            audio_path      TEXT,
            audio_secs      REAL,

            -- assemble stage
            output_path     TEXT,
            thumb_path      TEXT,

            -- metadata stage
            yt_title        TEXT,
            yt_description  TEXT,
            yt_tags         TEXT,   -- newline-joined blob; uploader splits on \\n

            -- upload + thumbnail stages
            youtube_id      TEXT,
            thumb_set       INTEGER NOT NULL DEFAULT 0,

            created_at      INTEGER NOT NULL DEFAULT (strftime('%s','now')),

            FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
        );

        -- one frame per book for this pipeline; flip to a composite key if you
        -- ever cut multiple videos from one book.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_frames_book ON script_frames(book_id);

        -- the metadata/upload fetch queries filter on these; index keeps the
        -- 'pending work' scans cheap as the table grows.
        CREATE INDEX IF NOT EXISTS idx_frames_output  ON script_frames(output_path);
        CREATE INDEX IF NOT EXISTS idx_frames_youtube ON script_frames(youtube_id);",
    )
    .context("creating script_frames table")?;
    Ok(())
}

// ============================================================
// pipeline_state — crash-resumable per-stage job tracking
// ============================================================
//
// This is what state.rs drives: mark_running before spawning, mark_done /
// mark_failed after. A crash mid-batch leaves rows in 'running'; a reaper (or
// a startup sweep) can reset stale 'running' rows back to 'pending'.

fn create_pipeline_state(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS pipeline_state (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            stage       TEXT    NOT NULL,   -- 'hook' | 'tts' | 'assemble' | 'metadata' | 'upload' | 'thumbnail'
            book_id     INTEGER NOT NULL,
            status      TEXT    NOT NULL DEFAULT 'pending', -- pending|running|done|failed
            attempts    INTEGER NOT NULL DEFAULT 0,
            last_error  TEXT,
            updated_at  INTEGER NOT NULL DEFAULT (strftime('%s','now')),

            FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE
        );

        -- one state row per (stage, book): the UNIQUE here lets state.rs use
        -- INSERT OR IGNORE to claim a job and UPDATE ... WHERE status='pending'
        -- to transition it without races.
        CREATE UNIQUE INDEX IF NOT EXISTS idx_state_stage_book
            ON pipeline_state(stage, book_id);

        -- the 'give me the next N pending jobs for stage X' query lives here.
        CREATE INDEX IF NOT EXISTS idx_state_stage_status
            ON pipeline_state(stage, status);",
    )
    .context("creating pipeline_state table")?;
    Ok(())
}

// ============================================================
// channel_meta — single-row niche attribution for this DB
// ============================================================

fn create_channel_meta(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS channel_meta (
            id     INTEGER PRIMARY KEY CHECK (id = 1),  -- enforce one row
            niche  TEXT NOT NULL,
            format TEXT NOT NULL
        );",
    )
    .context("creating channel_meta table")?;
    Ok(())
}

// ============================================================
// video_stats — per-video performance time series (analytics)
// ============================================================
//
// One row per (video_id, snapshot_date). analytics appends a fresh snapshot
// each run; selector reads the latest snapshot per video to score niches.

fn create_video_stats(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS video_stats (
            video_id            TEXT NOT NULL,
            snapshot_date       TEXT NOT NULL,   -- ISO YYYY-MM-DD
            views               INTEGER NOT NULL DEFAULT 0,
            est_minutes_watched INTEGER NOT NULL DEFAULT 0,
            avg_view_duration   REAL    NOT NULL DEFAULT 0,
            avg_view_percentage REAL    NOT NULL DEFAULT 0,
            likes               INTEGER NOT NULL DEFAULT 0,
            comments            INTEGER NOT NULL DEFAULT 0,
            subscribers_gained  INTEGER NOT NULL DEFAULT 0,
            est_revenue_usd     REAL    NOT NULL DEFAULT 0,
            cpm_usd             REAL    NOT NULL DEFAULT 0,
            PRIMARY KEY (video_id, snapshot_date)
        );
        CREATE INDEX IF NOT EXISTS idx_stats_video ON video_stats(video_id);",
    )
    .context("creating video_stats table")?;
    Ok(())
}

// ============================================================
// production_plan — selector output, per cycle
// ============================================================

fn create_production_plan(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS production_plan (
            run_date  TEXT NOT NULL,   -- ISO YYYY-MM-DD
            niche     TEXT NOT NULL,
            format    TEXT NOT NULL,
            quota     INTEGER NOT NULL,
            reason    TEXT NOT NULL,
            PRIMARY KEY (run_date, niche, format)
        );",
    )
    .context("creating production_plan table")?;
    Ok(())
}

// ============================================================
// Additive migrations
// ============================================================

/// Columns added after a table already shipped. Each ALTER is wrapped so a
/// "duplicate column name" error on a DB that already has it is swallowed —
/// that's how you get idempotent additive migrations in SQLite, which has no
/// "ADD COLUMN IF NOT EXISTS".
fn migrate_additive(conn: &Connection) -> Result<()> {
    let additive = [
        // (table, column-with-type) — append new ones here over time.
        ("books", "issued_year INTEGER"),
        ("books", "subjects TEXT NOT NULL DEFAULT ''"),
        ("script_frames", "thumb_path TEXT"),
        ("script_frames", "thumb_set INTEGER NOT NULL DEFAULT 0"),
        ("pipeline_state", "last_error TEXT"),
    ];

    for (table, coldef) in additive {
        let sql = format!("ALTER TABLE {table} ADD COLUMN {coldef}");
        match conn.execute(&sql, []) {
            Ok(_) => {}
            Err(rusqlite::Error::SqliteFailure(_, Some(msg)))
                if msg.contains("duplicate column name") =>
            { /* already present */ }
            Err(e) => return Err(e).with_context(|| format!("ALTER {table}: {coldef}")),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_is_idempotent() {
        let conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        // second run must not error
        init(&conn).unwrap();
    }

    #[test]
    fn books_unique_gutenberg_id_enforced() {
        let conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        conn.execute(
            "INSERT INTO books (gutenberg_id, title) VALUES (1, 'A')",
            [],
        )
        .unwrap();
        // second insert with same gutenberg_id without OR IGNORE must fail
        let dup = conn.execute(
            "INSERT INTO books (gutenberg_id, title) VALUES (1, 'B')",
            [],
        );
        assert!(dup.is_err());
    }

    #[test]
    fn insert_or_ignore_dedupes() {
        let conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        for _ in 0..3 {
            conn.execute(
                "INSERT OR IGNORE INTO books (gutenberg_id, title) VALUES (7, 'X')",
                [],
            )
            .unwrap();
        }
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM books", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
    }
}
