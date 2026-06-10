// src/selector.rs
//
// The actuator half of the feedback loop. analytics measures; selector decides.
// It reads the latest performance snapshot per video, scores each (niche,
// format) by revenue-per-video, blends an even "explore" split with a
// score-weighted "exploit" split, clamps the result into policy bounds, and
// writes the next cycle's production_plan. ingest/hook then read their batch
// size from quota_for() instead of a hardcoded limit.

use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::BTreeMap;

use crate::config::SelectorConfig;

/// (niche, format) — the grain production is allocated at.
pub type NicheKey = (String, String);

/// Pure allocator core. No I/O, fully unit-testable.
///
/// * `scores`  — revenue-per-video per niche/format (higher = better).
/// * `samples` — number of videos backing each score (the confidence signal).
///
/// Niches under `min_sample` are forced onto the explore track (even share) so
/// a single lucky or unlucky early video can't dictate the budget. Everyone
/// else gets `exploit_weight`·(score share) + (1−weight)·(even share), clamped
/// to `[min_per_niche, max_per_niche]`.
pub fn allocate(
    scores: &BTreeMap<NicheKey, f64>,
    samples: &BTreeMap<NicheKey, i64>,
    cfg: &SelectorConfig,
) -> Vec<(String, String, i64, String)> {
    let keys: Vec<&NicheKey> = scores.keys().collect();
    let n = keys.len();
    if n == 0 {
        return Vec::new();
    }

    let even = cfg.total_budget as f64 / n as f64;
    let w = cfg.exploit_weight.clamp(0.0, 1.0);
    let score_sum: f64 = keys
        .iter()
        .map(|k| scores.get(*k).copied().unwrap_or(0.0).max(0.0))
        .sum();

    let mut out = Vec::with_capacity(n);
    for k in keys {
        let score = scores.get(k).copied().unwrap_or(0.0).max(0.0);
        let sample = samples.get(k).copied().unwrap_or(0);

        let (raw, reason) = if sample < cfg.min_sample {
            (
                even,
                format!("explore: {sample} sample(s) < {} min", cfg.min_sample),
            )
        } else {
            let exploit_share = if score_sum > 0.0 {
                (score / score_sum) * cfg.total_budget as f64
            } else {
                even
            };
            let blended = w * exploit_share + (1.0 - w) * even;
            (
                blended,
                format!("exploit: score={score:.4} blend(w={w:.2})"),
            )
        };

        let quota = (raw.round() as i64).clamp(cfg.min_per_niche, cfg.max_per_niche);
        out.push((k.0.clone(), k.1.clone(), quota, reason));
    }
    out
}

/// Latest-snapshot revenue-per-video and sample count per (niche, format),
/// derived from this DB's `video_stats` joined to its single `channel_meta`.
pub fn score_niches(
    conn: &Connection,
) -> Result<(BTreeMap<NicheKey, f64>, BTreeMap<NicheKey, i64>)> {
    let mut stmt = conn.prepare(
        "SELECT cm.niche, cm.format,
                SUM(latest.est_revenue_usd) AS rev,
                COUNT(*) AS videos
           FROM (
                SELECT vs.video_id, vs.est_revenue_usd
                  FROM video_stats vs
                  JOIN (SELECT video_id, MAX(snapshot_date) md
                          FROM video_stats GROUP BY video_id) m
                    ON m.video_id = vs.video_id AND m.md = vs.snapshot_date
           ) latest
           JOIN script_frames sf ON sf.youtube_id = latest.video_id
           CROSS JOIN channel_meta cm
          GROUP BY cm.niche, cm.format",
    )?;

    let rows = stmt.query_map([], |r| {
        let niche: String = r.get(0)?;
        let format: String = r.get(1)?;
        let rev: f64 = r.get(2)?;
        let videos: i64 = r.get(3)?;
        Ok((niche, format, rev, videos))
    })?;

    let mut scores = BTreeMap::new();
    let mut samples = BTreeMap::new();
    for row in rows {
        let (niche, format, rev, videos) = row?;
        let key = (niche, format);
        let per_video = if videos > 0 { rev / videos as f64 } else { 0.0 };
        scores.insert(key.clone(), per_video);
        samples.insert(key, videos);
    }
    Ok((scores, samples))
}

/// Upsert the plan rows for `run_date` (idempotent per cycle).
pub fn write_plan(
    conn: &Connection,
    run_date: &str,
    plan: &[(String, String, i64, String)],
) -> Result<()> {
    for (niche, format, quota, reason) in plan {
        conn.execute(
            "INSERT INTO production_plan (run_date, niche, format, quota, reason)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(run_date, niche, format) DO UPDATE
               SET quota = excluded.quota, reason = excluded.reason",
            params![run_date, niche, format, quota, reason],
        )
        .with_context(|| format!("writing plan for {niche}/{format}"))?;
    }
    Ok(())
}

/// The quota ingest/hook should target for this niche/format on `run_date`.
/// Returns the most recent plan at or before that date, or None if no plan
/// exists yet (caller falls back to its CLI limit).
pub fn quota_for(
    conn: &Connection,
    run_date: &str,
    niche: &str,
    format: &str,
) -> Result<Option<i64>> {
    let q: Option<i64> = conn
        .query_row(
            "SELECT quota FROM production_plan
              WHERE niche = ?2 AND format = ?3 AND run_date <= ?1
              ORDER BY run_date DESC LIMIT 1",
            params![run_date, niche, format],
            |r| r.get(0),
        )
        .optional()
        .context("reading quota_for")?;
    Ok(q)
}

/// Score → allocate → persist for one cycle. When there's no performance data
/// yet, emit a single baseline row at `min_per_niche` for this DB's channel so
/// `quota_for` still returns something and the loop can bootstrap.
pub fn run(conn: &Connection, cfg: &SelectorConfig, run_date: &str) -> Result<usize> {
    let (scores, samples) = score_niches(conn)?;

    let plan = if scores.is_empty() {
        match channel_key(conn)? {
            Some((niche, format)) => vec![(
                niche,
                format,
                cfg.min_per_niche,
                "baseline: no performance data yet".to_string(),
            )],
            None => Vec::new(),
        }
    } else {
        allocate(&scores, &samples, cfg)
    };

    write_plan(conn, run_date, &plan)?;
    Ok(plan.len())
}

fn channel_key(conn: &Connection) -> Result<Option<NicheKey>> {
    conn.query_row(
        "SELECT niche, format FROM channel_meta WHERE id = 1",
        [],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
    )
    .optional()
    .context("reading channel_meta")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> SelectorConfig {
        SelectorConfig {
            total_budget: 100,
            min_per_niche: 5,
            max_per_niche: 60,
            min_sample: 10,
            exploit_weight: 0.7,
        }
    }

    #[test]
    fn low_sample_niches_go_explore_and_are_clamped() {
        let mut scores = BTreeMap::new();
        let mut samples = BTreeMap::new();
        scores.insert(("a".into(), "long".into()), 100.0);
        samples.insert(("a".into(), "long".into()), 2); // under min_sample
        let plan = allocate(&scores, &samples, &cfg());
        assert_eq!(plan.len(), 1);
        // even share (100) clamped to max_per_niche (60)
        assert_eq!(plan[0].2, 60);
        assert!(plan[0].3.contains("explore"));
    }

    #[test]
    fn winner_gets_more_than_loser_when_sampled() {
        let mut scores = BTreeMap::new();
        let mut samples = BTreeMap::new();
        scores.insert(("win".into(), "long".into()), 9.0);
        scores.insert(("lose".into(), "long".into()), 1.0);
        samples.insert(("win".into(), "long".into()), 20);
        samples.insert(("lose".into(), "long".into()), 20);
        let plan = allocate(&scores, &samples, &cfg());
        let win = plan.iter().find(|p| p.0 == "win").unwrap().2;
        let lose = plan.iter().find(|p| p.0 == "lose").unwrap().2;
        assert!(win > lose, "winner {win} should exceed loser {lose}");
    }

    #[test]
    fn quotas_respect_bounds() {
        let mut scores = BTreeMap::new();
        let mut samples = BTreeMap::new();
        scores.insert(("only".into(), "short".into()), 1000.0);
        samples.insert(("only".into(), "short".into()), 50);
        let plan = allocate(&scores, &samples, &cfg());
        assert!(plan[0].2 >= 5 && plan[0].2 <= 60);
    }
}
