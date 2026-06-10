// src/analytics.rs
//
// The sensor half of the feedback loop. For every published video it pulls a
// performance snapshot from the YouTube Analytics API and appends one row per
// (video_id, snapshot_date) to video_stats — a time series, so selector can
// reason about velocity, not just a stale lifetime number.
//
// Reporting lag: revenue isn't final for ~2 days, so the query window ends at
// now − `until_days_ago` (default 2) to avoid logging phantom $0.00s as truth.

use anyhow::{anyhow, Context, Result};
use chrono::{Duration as ChronoDuration, Utc};
use rusqlite::{params, Connection};
use std::time::Duration;

use crate::auth::{self, OAuthConfig};
use crate::config::AppConfig;
use crate::throttle::Throttle;

const HOST: &str = "youtubeanalytics.googleapis.com";
const REPORTS_URL: &str = "https://youtubeanalytics.googleapis.com/v2/reports";

/// Metrics requested, in order. The last two (estimatedRevenue, cpm) require
/// the monetary scope and a monetized channel; they come back 0 otherwise.
const METRICS: &str = "views,estimatedMinutesWatched,averageViewDuration,\
averageViewPercentage,likes,comments,subscribersGained,estimatedRevenue,cpm";

#[derive(Debug, Default, Clone)]
struct Stats {
    views: i64,
    est_minutes_watched: i64,
    avg_view_duration: f64,
    avg_view_percentage: f64,
    likes: i64,
    comments: i64,
    subscribers_gained: i64,
    est_revenue_usd: f64,
    cpm_usd: f64,
}

#[derive(serde::Deserialize)]
struct ReportResponse {
    #[serde(default)]
    rows: Vec<Vec<serde_json::Value>>,
}

/// Pull a fresh snapshot for every published video and append to video_stats.
/// Returns the number of videos successfully recorded.
pub async fn run(conn: &Connection, cfg: &AppConfig, until_days_ago: i64) -> Result<usize> {
    let oauth = OAuthConfig::from_values(
        &cfg.auth.client_id,
        &cfg.auth.client_secret,
        &cfg.auth.refresh_token,
    )
    .context("building OAuthConfig for analytics")?;

    let client = reqwest::Client::new();
    // Single-flight at 250ms — the daily quota is generous, this just stays
    // off the rate-limit radar during a sweep.
    let throttle = Throttle::new(1, Duration::from_millis(250));

    let end = (Utc::now() - ChronoDuration::days(until_days_ago.max(0)))
        .format("%Y-%m-%d")
        .to_string();
    let start = "2005-02-14"; // YouTube's launch — effectively lifetime-to-date
    let snapshot = Utc::now().format("%Y-%m-%d").to_string();

    let video_ids = published_video_ids(conn)?;
    if video_ids.is_empty() {
        eprintln!("analytics: no published videos to sample");
        return Ok(0);
    }

    let mut wrote = 0usize;
    for vid in video_ids {
        let _guard = throttle.acquire(HOST).await;
        let token = auth::fetch_access_token(&client, &oauth)
            .await
            .context("minting analytics access token")?;

        match fetch_video(&client, &token, &vid, start, &end).await {
            Ok(stats) => {
                upsert_stats(conn, &vid, &snapshot, &stats)?;
                wrote += 1;
            }
            Err(e) => eprintln!("analytics: video {vid} failed: {e:#}"),
        }
    }

    eprintln!("analytics: recorded {wrote} snapshot(s) for {snapshot}");
    Ok(wrote)
}

fn published_video_ids(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT youtube_id FROM script_frames \
         WHERE youtube_id IS NOT NULL AND youtube_id <> ''",
    )?;
    let ids = stmt
        .query_map([], |r| r.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<String>>>()?;
    Ok(ids)
}

async fn fetch_video(
    client: &reqwest::Client,
    token: &str,
    video_id: &str,
    start: &str,
    end: &str,
) -> Result<Stats> {
    let resp = client
        .get(REPORTS_URL)
        .bearer_auth(token)
        .query(&[
            ("ids", "channel==MINE"),
            ("startDate", start),
            ("endDate", end),
            ("metrics", METRICS),
            ("filters", &format!("video=={video_id}")),
        ])
        .timeout(Duration::from_secs(30))
        .send()
        .await
        .context("requesting analytics report")?
        .error_for_status()
        .context("non-2xx from analytics API")?
        .json::<ReportResponse>()
        .await
        .context("parsing analytics report")?;

    let row = resp
        .rows
        .into_iter()
        .next()
        .ok_or_else(|| anyhow!("no analytics rows for video {video_id}"))?;

    Ok(parse_row(&row))
}

/// Map the ordered metric values onto Stats, tolerating short rows.
fn parse_row(row: &[serde_json::Value]) -> Stats {
    let int_at = |i: usize| -> i64 { row.get(i).and_then(|v| v.as_i64()).unwrap_or(0) };
    let f_at = |i: usize| -> f64 { row.get(i).and_then(|v| v.as_f64()).unwrap_or(0.0) };
    Stats {
        views: int_at(0),
        est_minutes_watched: int_at(1),
        avg_view_duration: f_at(2),
        avg_view_percentage: f_at(3),
        likes: int_at(4),
        comments: int_at(5),
        subscribers_gained: int_at(6),
        est_revenue_usd: f_at(7),
        cpm_usd: f_at(8),
    }
}

fn upsert_stats(conn: &Connection, video_id: &str, snapshot: &str, s: &Stats) -> Result<()> {
    conn.execute(
        "INSERT INTO video_stats (
            video_id, snapshot_date, views, est_minutes_watched, avg_view_duration,
            avg_view_percentage, likes, comments, subscribers_gained,
            est_revenue_usd, cpm_usd
         ) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
         ON CONFLICT(video_id, snapshot_date) DO UPDATE SET
            views = excluded.views,
            est_minutes_watched = excluded.est_minutes_watched,
            avg_view_duration = excluded.avg_view_duration,
            avg_view_percentage = excluded.avg_view_percentage,
            likes = excluded.likes,
            comments = excluded.comments,
            subscribers_gained = excluded.subscribers_gained,
            est_revenue_usd = excluded.est_revenue_usd,
            cpm_usd = excluded.cpm_usd",
        params![
            video_id,
            snapshot,
            s.views,
            s.est_minutes_watched,
            s.avg_view_duration,
            s.avg_view_percentage,
            s.likes,
            s.comments,
            s.subscribers_gained,
            s.est_revenue_usd,
            s.cpm_usd,
        ],
    )
    .with_context(|| format!("recording stats for {video_id}"))?;
    Ok(())
}
