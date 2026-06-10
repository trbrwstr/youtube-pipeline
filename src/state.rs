// src/state.rs  (upload integration)

/// Returns Some(id) if this book already has a recorded YouTube id — meaning
/// the upload either finished or was confirmed earlier. The orchestrator calls
/// this BEFORE upload_video and short-circuits on a hit.
pub fn existing_youtube_id(conn: &rusqlite::Connection, book_id: i64) -> Result<Option<String>> {
    let id: Option<String> = conn
        .query_row(
            "SELECT youtube_id FROM script_frames \
             WHERE book_id = ?1 AND youtube_id IS NOT NULL AND youtube_id <> ''",
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
        "UPDATE script_frames SET youtube_id = ?1 WHERE book_id = ?2",
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
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;

/// Any row stuck in `running` longer than `stale_secs` is assumed to be from a
/// crashed process (the worker died before stamping success/failure). Flip it
/// back to `failed` so a retry pass can pick it up, and bump its attempt count
/// so a perpetually-crashing item eventually hits `dead`.
pub fn reap_stale(conn: &Connection, stale_secs: i64, max_attempts: i64) -> Result<usize> {
    // failed if under the attempt ceiling, dead if it's burned through them.
    let n = conn
        .execute(
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
    let n = conn
        .execute(
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

// ============================================================
// Unified per-item state machine
// ============================================================
//
// Both binaries drive every non-ingest stage through this trio:
//   eligible_for_stage -> [mark_running -> run_one -> mark_done|mark_failed]*
// pipeline_state is seeded lazily: a stage's pending rows appear once its
// upstream dependency is `done` (or, for the first real stage, once a book
// with a body exists). Each run_one carries its own idempotency guard, so a
// re-run is always safe.

/// One status label. Re-exported at the crate root for callers that want the
/// vocabulary without the SQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StageStatus {
    Pending,
    Running,
    Done,
    Failed,
    Dead,
}

/// Per-stage tally for the ops `status` grid.
#[derive(Debug, Default, Clone)]
pub struct StageCounts {
    pub pending: i64,
    pub running: i64,
    pub done: i64,
    pub failed: i64,
    pub dead: i64,
}

/// Seed missing `pending` rows for `stage`, then return up to `limit` book_ids
/// whose `stage` row is workable (pending or failed, under the attempt ceiling).
///
/// `depends_on` is the upstream stage that must be `done` first. The synthetic
/// "ingest" dependency is treated as "none" — ingest is a batch stage with no
/// per-book state — so the first real stage (`hook`) seeds straight off any
/// book that has a body.
pub fn eligible_for_stage(
    conn: &Connection,
    stage: &str,
    depends_on: Option<&str>,
    max_attempts: i64,
    limit: usize,
) -> Result<Vec<i64>> {
    let effective_dep = depends_on.filter(|d| *d != "ingest");

    // The `NOT EXISTS` guard keeps steady-state seeding cheap: once a book has
    // a row for this stage it's skipped, so each run only inserts genuinely-new
    // candidates instead of re-attempting an INSERT for every book every time.
    match effective_dep {
        Some(dep) => {
            conn.execute(
                "INSERT INTO pipeline_state (stage, book_id, status)
                 SELECT ?1, ps.book_id, 'pending'
                   FROM pipeline_state ps
                  WHERE ps.stage = ?2 AND ps.status = 'done'
                    AND NOT EXISTS (
                        SELECT 1 FROM pipeline_state x
                         WHERE x.stage = ?1 AND x.book_id = ps.book_id
                    )",
                params![stage, dep],
            )
            .with_context(|| format!("seeding {stage} from {dep}"))?;
        }
        None => {
            conn.execute(
                "INSERT INTO pipeline_state (stage, book_id, status)
                 SELECT ?1, b.id, 'pending'
                   FROM books b
                  WHERE b.body IS NOT NULL AND length(trim(b.body)) > 0
                    AND NOT EXISTS (
                        SELECT 1 FROM pipeline_state x
                         WHERE x.stage = ?1 AND x.book_id = b.id
                    )",
                params![stage],
            )
            .with_context(|| format!("seeding {stage} from books"))?;
        }
    }

    let mut stmt = conn.prepare(
        "SELECT book_id FROM pipeline_state
          WHERE stage = ?1 AND status IN ('pending','failed') AND attempts < ?2
          ORDER BY book_id
          LIMIT ?3",
    )?;
    let ids: Vec<i64> = stmt
        .query_map(params![stage, max_attempts, limit as i64], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    Ok(ids)
}

/// Claim a job: flip its row to `running` (creating it if somehow absent).
pub fn mark_running(conn: &Connection, book_id: i64, stage: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO pipeline_state (stage, book_id, status, updated_at)
         VALUES (?2, ?1, 'running', strftime('%s','now'))
         ON CONFLICT(stage, book_id) DO UPDATE
           SET status = 'running', updated_at = strftime('%s','now')",
        params![book_id, stage],
    )
    .with_context(|| format!("mark_running {stage} book {book_id}"))?;
    Ok(())
}

/// Terminal success: `done`, clear the error, bump the attempt count.
pub fn mark_done(conn: &Connection, book_id: i64, stage: &str) -> Result<()> {
    conn.execute(
        "UPDATE pipeline_state
            SET status = 'done', attempts = attempts + 1, last_error = NULL,
                updated_at = strftime('%s','now')
          WHERE stage = ?2 AND book_id = ?1",
        params![book_id, stage],
    )
    .with_context(|| format!("mark_done {stage} book {book_id}"))?;
    Ok(())
}

/// Terminal failure: `failed` while under the attempt ceiling, `dead` once it's
/// burned through them. Records the error for the dead-letter view.
pub fn mark_failed(
    conn: &Connection,
    book_id: i64,
    stage: &str,
    err: &str,
    max_attempts: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE pipeline_state
            SET status = CASE WHEN attempts + 1 >= ?3 THEN 'dead' ELSE 'failed' END,
                attempts = attempts + 1,
                last_error = ?4,
                updated_at = strftime('%s','now')
          WHERE stage = ?2 AND book_id = ?1",
        params![book_id, stage, max_attempts, err],
    )
    .with_context(|| format!("mark_failed {stage} book {book_id}"))?;
    Ok(())
}

/// Counts by status for a single stage (the ops `status` grid row).
pub fn stage_counts_for(conn: &Connection, stage: &str) -> Result<StageCounts> {
    let mut stmt = conn
        .prepare("SELECT status, COUNT(*) FROM pipeline_state WHERE stage = ?1 GROUP BY status")?;
    let rows = stmt.query_map(params![stage], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    })?;

    let mut c = StageCounts::default();
    for row in rows {
        let (status, n) = row?;
        match status.as_str() {
            "pending" => c.pending = n,
            "running" => c.running = n,
            "done" => c.done = n,
            "failed" => c.failed = n,
            "dead" => c.dead = n,
            _ => {}
        }
    }
    Ok(c)
}

#[cfg(test)]
mod machine_tests {
    use super::*;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        crate::db::init(&conn).unwrap();
        conn
    }

    fn add_book(conn: &Connection, id: i64, body: &str) {
        conn.execute(
            "INSERT INTO books (gutenberg_id, title, body) VALUES (?1, ?2, ?3)",
            params![id, format!("Book {id}"), body],
        )
        .unwrap();
    }

    #[test]
    fn hook_seeds_only_books_with_body() {
        let conn = db();
        add_book(&conn, 1, "a real body long enough");
        add_book(&conn, 2, ""); // empty body — must not be seeded
        let ids = eligible_for_stage(&conn, "hook", Some("ingest"), 3, 10).unwrap();
        assert_eq!(ids.len(), 1);
    }

    #[test]
    fn downstream_waits_for_dependency_done() {
        let conn = db();
        add_book(&conn, 1, "body");
        // hook not done yet -> tts has nothing eligible
        let ids = eligible_for_stage(&conn, "tts", Some("hook"), 3, 10).unwrap();
        assert!(ids.is_empty());

        // run hook to done, then tts becomes eligible
        let hook_ids = eligible_for_stage(&conn, "hook", Some("ingest"), 3, 10).unwrap();
        for id in hook_ids {
            mark_running(&conn, id, "hook").unwrap();
            mark_done(&conn, id, "hook").unwrap();
        }
        let ids = eligible_for_stage(&conn, "tts", Some("hook"), 3, 10).unwrap();
        assert_eq!(ids, vec![1]);
    }

    #[test]
    fn failed_becomes_dead_at_ceiling() {
        let conn = db();
        add_book(&conn, 1, "body");
        let _ = eligible_for_stage(&conn, "hook", None, 2, 10).unwrap();
        mark_running(&conn, 1, "hook").unwrap();
        mark_failed(&conn, 1, "hook", "boom", 2).unwrap(); // attempts 1 -> failed
        mark_failed(&conn, 1, "hook", "boom", 2).unwrap(); // attempts 2 -> dead
        let c = stage_counts_for(&conn, "hook").unwrap();
        assert_eq!(c.dead, 1);
        assert_eq!(c.failed, 0);
        // dead rows are not re-offered
        let ids = eligible_for_stage(&conn, "hook", None, 2, 10).unwrap();
        assert!(ids.is_empty());
    }
}
