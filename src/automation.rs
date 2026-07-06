// src/automation.rs
//
// Persistent automation state for the dashboard's server-side scheduler.
//
// The dashboard is the only long-running process in a fully-automated deploy,
// so this module gives it a durable place to keep "what should run by itself,
// how often, and what happened last time" — separate from the per-niche
// pipeline DBs, which stay pure production-domain data.
//
// Three tables in one small ops DB (default: data/dashboard.db):
//   automation_niche - per-niche schedule: enabled, cadence, batch size, flags
//   automation_fleet - single row: master pause + daily selector schedule
//   automation_runs  - append-only history of every automated cycle
//
// Scheduling is deliberately dumb-and-durable: the dashboard ticks every ~30s,
// asks `niche_due` / `selector_due`, and stamps `last_run_at` up front so a
// crashing cycle can't hot-loop. No cron parsing, no missed-tick recovery
// games — an interval and a timestamp survive restarts and are easy to reason
// about at 3am.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

// ============================================================
// Settings records
// ============================================================

/// Per-niche automation schedule. `niche` is the config file stem — the same
/// key the dashboard routes use — not `channel.niche`, so one settings row
/// always maps to exactly one TOML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NicheAutomation {
    pub niche: String,
    /// Master switch for this niche's automated cycles.
    pub enabled: bool,
    /// Minutes between cycle starts. Floor of 5 enforced on write.
    pub interval_minutes: i64,
    /// Per-stage item ceiling for each cycle (selector quota still caps
    /// ingest/hook below this).
    pub batch_limit: i64,
    /// When false the cycle stops before upload — full dry-run production.
    pub upload_enabled: bool,
    /// Pull an analytics snapshot at the end of each cycle.
    pub run_analytics: bool,
    /// Unix seconds of the last cycle START (stamped up front, see module doc).
    pub last_run_at: Option<i64>,
}

impl NicheAutomation {
    /// Defaults for a niche that has a config TOML but no settings row yet:
    /// automation off until a human flips it on, sane cadence once they do.
    pub fn defaults(niche: &str) -> Self {
        Self {
            niche: niche.to_string(),
            enabled: false,
            interval_minutes: 360,
            batch_limit: 25,
            upload_enabled: true,
            run_analytics: true,
            last_run_at: None,
        }
    }

    /// Next scheduled start, once enabled. None while disabled; an enabled
    /// niche that has never run is due immediately (next = now, i.e. epoch 0).
    pub fn next_run_at(&self) -> Option<i64> {
        if !self.enabled {
            return None;
        }
        Some(match self.last_run_at {
            Some(last) => last + self.interval_minutes * 60,
            None => 0,
        })
    }
}

/// Fleet-wide knobs: one row. `paused` is the big red switch that halts every
/// automated action without touching per-niche settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetAutomation {
    pub paused: bool,
    /// Run the federated selector (revenue-weighted quota allocation) daily.
    pub selector_enabled: bool,
    /// UTC hour (0-23) after which today's selector pass may fire.
    pub selector_hour_utc: i64,
    /// ISO date of the last selector pass; guards once-per-day.
    pub last_selector_date: Option<String>,
}

impl Default for FleetAutomation {
    fn default() -> Self {
        Self {
            paused: false,
            selector_enabled: true,
            selector_hour_utc: 6,
            last_selector_date: None,
        }
    }
}

/// One row of run history: an automated (or UI-triggered) cycle for a niche,
/// or a fleet-wide selector pass (niche = "fleet").
#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub id: i64,
    pub niche: String,
    /// "auto" (scheduler), "manual" (Run Now button), "selector".
    pub kind: String,
    /// "running" | "ok" | "error".
    pub status: String,
    pub message: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
}

// ============================================================
// Schema
// ============================================================

/// Open the ops DB (creating parent dirs) and apply the automation schema.
pub fn open(path: &str) -> Result<Connection> {
    if let Some(parent) = std::path::Path::new(path).parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating state db dir {}", parent.display()))?;
        }
    }
    let conn = Connection::open(path).with_context(|| format!("opening state db {path}"))?;
    conn.execute_batch(
        "PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA busy_timeout = 5000;",
    )
    .context("applying state db pragmas")?;
    init(&conn)?;
    Ok(conn)
}

/// Idempotent schema — mirrors db.rs's CREATE IF NOT EXISTS discipline.
pub fn init(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS automation_niche (
            niche            TEXT PRIMARY KEY,
            enabled          INTEGER NOT NULL DEFAULT 0,
            interval_minutes INTEGER NOT NULL DEFAULT 360,
            batch_limit      INTEGER NOT NULL DEFAULT 25,
            upload_enabled   INTEGER NOT NULL DEFAULT 1,
            run_analytics    INTEGER NOT NULL DEFAULT 1,
            last_run_at      INTEGER
        );

        CREATE TABLE IF NOT EXISTS automation_fleet (
            id                 INTEGER PRIMARY KEY CHECK (id = 1),
            paused             INTEGER NOT NULL DEFAULT 0,
            selector_enabled   INTEGER NOT NULL DEFAULT 1,
            selector_hour_utc  INTEGER NOT NULL DEFAULT 6,
            last_selector_date TEXT
        );

        CREATE TABLE IF NOT EXISTS automation_runs (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            niche       TEXT NOT NULL,
            kind        TEXT NOT NULL,
            status      TEXT NOT NULL,
            message     TEXT NOT NULL DEFAULT '',
            started_at  INTEGER NOT NULL,
            finished_at INTEGER
        );
        CREATE INDEX IF NOT EXISTS idx_runs_started ON automation_runs(started_at DESC);",
    )
    .context("creating automation tables")?;
    Ok(())
}

// ============================================================
// Fleet settings
// ============================================================

pub fn get_fleet(conn: &Connection) -> Result<FleetAutomation> {
    let row = conn
        .query_row(
            "SELECT paused, selector_enabled, selector_hour_utc, last_selector_date
               FROM automation_fleet WHERE id = 1",
            [],
            |r| {
                Ok(FleetAutomation {
                    paused: r.get::<_, i64>(0)? != 0,
                    selector_enabled: r.get::<_, i64>(1)? != 0,
                    selector_hour_utc: r.get(2)?,
                    last_selector_date: r.get(3)?,
                })
            },
        )
        .optional()
        .context("reading fleet automation settings")?;
    Ok(row.unwrap_or_default())
}

pub fn set_fleet(conn: &Connection, fleet: &FleetAutomation) -> Result<()> {
    conn.execute(
        "INSERT INTO automation_fleet
             (id, paused, selector_enabled, selector_hour_utc, last_selector_date)
         VALUES (1, ?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
             paused = excluded.paused,
             selector_enabled = excluded.selector_enabled,
             selector_hour_utc = excluded.selector_hour_utc,
             last_selector_date = excluded.last_selector_date",
        params![
            fleet.paused as i64,
            fleet.selector_enabled as i64,
            fleet.selector_hour_utc.clamp(0, 23),
            fleet.last_selector_date,
        ],
    )
    .context("writing fleet automation settings")?;
    Ok(())
}

/// Stamp today's selector pass as done (call when the pass STARTS, so an
/// erroring selector can't retry-storm every tick until midnight).
pub fn mark_selector_ran(conn: &Connection, date: &str) -> Result<()> {
    let mut fleet = get_fleet(conn)?;
    fleet.last_selector_date = Some(date.to_string());
    set_fleet(conn, &fleet)
}

/// The daily selector fires once per UTC date, at-or-after the configured hour.
pub fn selector_due(fleet: &FleetAutomation, today: &str, hour_utc: i64) -> bool {
    if fleet.paused || !fleet.selector_enabled {
        return false;
    }
    if hour_utc < fleet.selector_hour_utc {
        return false;
    }
    fleet.last_selector_date.as_deref() != Some(today)
}

// ============================================================
// Per-niche settings
// ============================================================

pub fn get_niche(conn: &Connection, niche: &str) -> Result<NicheAutomation> {
    let row = conn
        .query_row(
            "SELECT niche, enabled, interval_minutes, batch_limit,
                    upload_enabled, run_analytics, last_run_at
               FROM automation_niche WHERE niche = ?1",
            params![niche],
            map_niche_row,
        )
        .optional()
        .context("reading niche automation settings")?;
    Ok(row.unwrap_or_else(|| NicheAutomation::defaults(niche)))
}

pub fn upsert_niche(conn: &Connection, n: &NicheAutomation) -> Result<()> {
    conn.execute(
        "INSERT INTO automation_niche
             (niche, enabled, interval_minutes, batch_limit,
              upload_enabled, run_analytics, last_run_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(niche) DO UPDATE SET
             enabled = excluded.enabled,
             interval_minutes = excluded.interval_minutes,
             batch_limit = excluded.batch_limit,
             upload_enabled = excluded.upload_enabled,
             run_analytics = excluded.run_analytics,
             last_run_at = excluded.last_run_at",
        params![
            n.niche,
            n.enabled as i64,
            n.interval_minutes.max(5),
            n.batch_limit.clamp(1, 500),
            n.upload_enabled as i64,
            n.run_analytics as i64,
            n.last_run_at,
        ],
    )
    .context("writing niche automation settings")?;
    Ok(())
}

/// Stamp a cycle start. Writing this BEFORE the work runs is what stops a
/// failing niche from re-firing every scheduler tick.
pub fn mark_niche_ran(conn: &Connection, niche: &str, now: i64) -> Result<()> {
    let mut n = get_niche(conn, niche)?;
    n.last_run_at = Some(now);
    upsert_niche(conn, &n)
}

/// Is this niche's next cycle due at `now`? Pure so it's trivially testable.
pub fn niche_due(n: &NicheAutomation, now: i64) -> bool {
    n.enabled && n.next_run_at().is_some_and(|next| now >= next)
}

fn map_niche_row(r: &rusqlite::Row) -> rusqlite::Result<NicheAutomation> {
    Ok(NicheAutomation {
        niche: r.get(0)?,
        enabled: r.get::<_, i64>(1)? != 0,
        interval_minutes: r.get(2)?,
        batch_limit: r.get(3)?,
        upload_enabled: r.get::<_, i64>(4)? != 0,
        run_analytics: r.get::<_, i64>(5)? != 0,
        last_run_at: r.get(6)?,
    })
}

// ============================================================
// Run history
// ============================================================

/// Open a history row in `running` state; returns its id for the finish stamp.
pub fn record_run_start(conn: &Connection, niche: &str, kind: &str, now: i64) -> Result<i64> {
    conn.execute(
        "INSERT INTO automation_runs (niche, kind, status, message, started_at)
         VALUES (?1, ?2, 'running', 'started', ?3)",
        params![niche, kind, now],
    )
    .context("recording run start")?;
    Ok(conn.last_insert_rowid())
}

pub fn record_run_finish(
    conn: &Connection,
    run_id: i64,
    ok: bool,
    message: &str,
    now: i64,
) -> Result<()> {
    conn.execute(
        "UPDATE automation_runs
            SET status = ?2, message = ?3, finished_at = ?4
          WHERE id = ?1",
        params![run_id, if ok { "ok" } else { "error" }, message, now],
    )
    .context("recording run finish")?;
    Ok(())
}

pub fn recent_runs(conn: &Connection, limit: usize) -> Result<Vec<RunRecord>> {
    let mut stmt = conn.prepare(
        "SELECT id, niche, kind, status, message, started_at, finished_at
           FROM automation_runs
          ORDER BY started_at DESC, id DESC
          LIMIT ?1",
    )?;
    let rows = stmt
        .query_map(params![limit as i64], |r| {
            Ok(RunRecord {
                id: r.get(0)?,
                niche: r.get(1)?,
                kind: r.get(2)?,
                status: r.get(3)?,
                message: r.get(4)?,
                started_at: r.get(5)?,
                finished_at: r.get(6)?,
            })
        })?
        .collect::<rusqlite::Result<_>>()?;
    Ok(rows)
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        init(&conn).unwrap();
        conn
    }

    #[test]
    fn init_is_idempotent() {
        let conn = db();
        init(&conn).unwrap();
    }

    #[test]
    fn fleet_defaults_then_round_trips() {
        let conn = db();
        let fleet = get_fleet(&conn).unwrap();
        assert!(!fleet.paused);
        assert!(fleet.selector_enabled);

        let updated = FleetAutomation {
            paused: true,
            selector_enabled: false,
            selector_hour_utc: 14,
            last_selector_date: Some("2026-07-06".into()),
        };
        set_fleet(&conn, &updated).unwrap();
        let back = get_fleet(&conn).unwrap();
        assert!(back.paused);
        assert!(!back.selector_enabled);
        assert_eq!(back.selector_hour_utc, 14);
        assert_eq!(back.last_selector_date.as_deref(), Some("2026-07-06"));
    }

    #[test]
    fn niche_defaults_are_off_until_enabled() {
        let conn = db();
        let n = get_niche(&conn, "classics").unwrap();
        assert!(!n.enabled);
        assert_eq!(n.next_run_at(), None);
        assert!(!niche_due(&n, 999_999));
    }

    #[test]
    fn enabled_niche_never_run_is_due_immediately() {
        let n = NicheAutomation {
            enabled: true,
            ..NicheAutomation::defaults("classics")
        };
        assert!(niche_due(&n, 1));
    }

    #[test]
    fn niche_due_respects_interval() {
        let n = NicheAutomation {
            enabled: true,
            interval_minutes: 60,
            last_run_at: Some(1_000),
            ..NicheAutomation::defaults("classics")
        };
        assert!(!niche_due(&n, 1_000 + 3_599));
        assert!(niche_due(&n, 1_000 + 3_600));
    }

    #[test]
    fn upsert_clamps_and_round_trips() {
        let conn = db();
        let n = NicheAutomation {
            niche: "classics".into(),
            enabled: true,
            interval_minutes: 1, // below floor -> stored as 5
            batch_limit: 9_999,  // above ceiling -> stored as 500
            upload_enabled: false,
            run_analytics: false,
            last_run_at: Some(42),
        };
        upsert_niche(&conn, &n).unwrap();
        let back = get_niche(&conn, "classics").unwrap();
        assert!(back.enabled);
        assert_eq!(back.interval_minutes, 5);
        assert_eq!(back.batch_limit, 500);
        assert!(!back.upload_enabled);
        assert!(!back.run_analytics);
        assert_eq!(back.last_run_at, Some(42));
    }

    #[test]
    fn mark_niche_ran_stamps_start_time() {
        let conn = db();
        mark_niche_ran(&conn, "classics", 7_777).unwrap();
        let n = get_niche(&conn, "classics").unwrap();
        assert_eq!(n.last_run_at, Some(7_777));
        // stamping must not silently enable automation
        assert!(!n.enabled);
    }

    #[test]
    fn selector_fires_once_per_day_after_hour() {
        let mut fleet = FleetAutomation {
            selector_hour_utc: 6,
            ..Default::default()
        };
        assert!(!selector_due(&fleet, "2026-07-06", 5)); // too early
        assert!(selector_due(&fleet, "2026-07-06", 6)); // due
        fleet.last_selector_date = Some("2026-07-06".into());
        assert!(!selector_due(&fleet, "2026-07-06", 12)); // already ran today
        assert!(selector_due(&fleet, "2026-07-07", 6)); // next day due again
        fleet.paused = true;
        assert!(!selector_due(&fleet, "2026-07-07", 6)); // pause wins
    }

    #[test]
    fn run_history_round_trips() {
        let conn = db();
        let id = record_run_start(&conn, "classics", "auto", 100).unwrap();
        record_run_finish(&conn, id, true, "7 stage(s) completed", 160).unwrap();
        let id2 = record_run_start(&conn, "fleet", "selector", 200).unwrap();
        record_run_finish(&conn, id2, false, "boom", 201).unwrap();

        let runs = recent_runs(&conn, 10).unwrap();
        assert_eq!(runs.len(), 2);
        assert_eq!(runs[0].niche, "fleet");
        assert_eq!(runs[0].status, "error");
        assert_eq!(runs[1].status, "ok");
        assert_eq!(runs[1].finished_at, Some(160));
    }
}
