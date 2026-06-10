// src/state.rs  (upload integration)

/// Returns Some(id) if this book already has a recorded YouTube id — meaning
/// the upload either finished or was confirmed earlier. The orchestrator calls
/// this BEFORE upload_video and short-circuits on a hit.
pub fn existing_youtube_id(conn: &rusqlite::Connection, book_id: i64) -> Result<Option<String>> {
    let id: Option<String> = conn
        .query_row(
            "SELECT youtube_id FROM books WHERE id = ?1 AND youtube_id IS NOT NULL",
            [book_id],
            |row| row.get(0),
        )
        .optional()
        .context("checking for existing youtube_id")?;
    Ok(id)
}

/// Atomic completion: stamp the video id onto the book AND flip the upload
/// stage to done in ONE transaction. If the process dies between these two
/// writes you get a half-published row that re-runs can't reason about — the
/// transaction forbids that. ID lands first, status second, all-or-nothing.
pub fn mark_done_with_video(
    conn: &mut rusqlite::Connection,
    book_id: i64,
    stage: &str,
    youtube_id: &str,
) -> Result<()> {
    let tx = conn.transaction().context("opening upload-commit tx")?;
    tx.execute(
        "UPDATE books SET youtube_id = ?1 WHERE id = ?2",
        rusqlite::params![youtube_id, book_id],
    )
    .context("recording youtube_id")?;
    tx.execute(
        "UPDATE pipeline_state
            SET status = 'done', attempts = attempts + 1, last_error = NULL,
                updated_at = strftime('%s','now')
          WHERE book_id = ?1 AND stage = ?2",
        rusqlite::params![book_id, stage],
    )
    .context("marking upload stage done")?;
    tx.commit().context("committing upload completion")?;
    Ok(())
}
// src/state.rs (additions)

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use std::collections::BTreeMap;

/// Any row stuck in `running` longer than `stale_secs` is assumed to be from a
/// crashed process (the worker died before stamping success/failure). Flip it
/// back to `failed` so a retry pass can pick it up, and bump its attempt count
/// so a perpetually-crashing item eventually hits `dead`.
pub fn reap_stale(conn: &Connection, stale_secs: i64, max_attempts: i64) -> Result<usize> {
    // failed if under the attempt ceiling, dead if it's burned through them.
    let n = conn.execute(
        "UPDATE pipeline_state
            SET status = CASE
                    WHEN attempts + 1 >= ?2 THEN 'dead'
                    ELSE 'failed'
                END,
                attempts = attempts + 1,
                last_error = COALESCE(last_error, 'reaped: stale running row'),
                updated_at = strftime('%s','now')
          WHERE status = 'running'
            AND updated_at < strftime('%s','now') - ?1",
        params![stale_secs, max_attempts],
    )
    .context("reaping stale running rows")?;
    Ok(n)
}

/// (stage, status) -> count, for the status grid.
pub fn stage_counts(conn: &Connection) -> Result<BTreeMap<(String, String), i64>> {
    let mut stmt = conn.prepare(
        "SELECT stage, status, COUNT(*)
           FROM pipeline_state
          GROUP BY stage, status",
    )?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, i64>(2)?,
        ))
    })?;

    let mut map = BTreeMap::new();
    for row in rows {
        let (stage, status, count) = row?;
        map.insert((stage, status), count);
    }
    Ok(map)
}

/// Re-queue every `failed` row for one stage back to `pending` so the next
/// `--stage X` run reprocesses them. Deliberately does NOT touch `dead` rows —
/// those exceeded max_attempts and need manual eyes. Returns rows re-queued.
pub fn retry_stage(conn: &Connection, stage: &str) -> Result<usize> {
    let n = conn.execute(
        "UPDATE pipeline_state
            SET status = 'pending',
                last_error = NULL,
                updated_at = strftime('%s','now')
          WHERE stage = ?1
            AND status = 'failed'",
        params![stage],
    )
    .context("re-queuing failed rows")?;
    Ok(n)
}

/// Pull the dead-letter rows for inspection: items that exhausted retries.
#[derive(Debug)]
pub struct DeadRow {
    pub book_id: i64,
    pub stage: String,
    pub attempts: i64,
    pub last_error: String,
    pub updated_at: i64,
}

pub fn dead_letters(conn: &Connection, stage: Option<&str>, limit: usize) -> Result<Vec<DeadRow>> {
    let (sql, has_filter) = match stage {
        Some(_) => (
            "SELECT book_id, stage, attempts, COALESCE(last_error,''), updated_at
               FROM pipeline_state
              WHERE status = 'dead' AND stage = ?1
              ORDER BY updated_at DESC
              LIMIT ?2",
            true,
        ),
        None => (
            "SELECT book_id, stage, attempts, COALESCE(last_error,''), updated_at
               FROM pipeline_state
              WHERE status = 'dead'
              ORDER BY updated_at DESC
              LIMIT ?1",
            false,
        ),
    };

    let mut stmt = conn.prepare(sql)?;
    let map = |r: &rusqlite::Row| {
        Ok(DeadRow {
            book_id: r.get(0)?,
            stage: r.get(1)?,
            attempts: r.get(2)?,
            last_error: r.get(3)?,
            updated_at: r.get(4)?,
        })
    };

    let rows: Vec<DeadRow> = if has_filter {
        stmt.query_map(params![stage.unwrap(), limit as i64], map)?
            .collect::<rusqlite::Result<_>>()?
    } else {
        stmt.query_map(params![limit as i64], map)?
            .collect::<rusqlite::Result<_>>()?
    };
    Ok(rows)
}