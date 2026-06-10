// src/runner.rs
//
// The shared, bounded-concurrency stage runner used by both binaries.
//
// Every non-ingest stage runs through `run_stage`: it seeds + selects eligible
// book_ids, then drains them across a fixed set of workers. Each worker owns
// its own SQLite connection (rusqlite::Connection is Send but not Sync, so a
// connection can't be shared across the await points of concurrent tasks);
// network I/O overlaps while SQLite serializes writes under WAL + busy_timeout.
//
// One `reqwest::Client` and one `Throttle` are built per stage run and cloned
// into every worker — clients share a connection pool, and the Throttle's
// global semaphore + per-host floors finally accumulate across items instead
// of being rebuilt per call.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use reqwest::Client;

use crate::config::AppConfig;
use crate::hook::CostGuard;
use crate::throttle::Throttle;
use crate::{assemble, db, hook, metadata, selector, state, thumbnail, tts, upload};

/// Run one non-ingest stage over its eligible items with bounded concurrency.
/// Returns the number of items processed (claimed and run; failures are stamped
/// but not counted). `limit` is the already-quota-resolved ceiling.
pub async fn run_stage(
    cfg: &AppConfig,
    stage: &str,
    depends_on: Option<&str>,
    limit: usize,
) -> Result<usize> {
    // Seed + select on a short-lived connection, then hand ids to the workers.
    let ids = {
        let conn = db::open_and_init(&cfg.db_path)
            .with_context(|| format!("opening db {}", cfg.db_path))?;
        state::eligible_for_stage(&conn, stage, depends_on, cfg.max_attempts, limit)
            .with_context(|| format!("selecting eligible items for {stage}"))?
    };
    if ids.is_empty() {
        return Ok(0);
    }

    let client = Client::new();
    let throttle = cfg.build_throttle();
    let max = cfg.throttle.max_concurrency.max(1);

    // Batch-wide LLM budget shared by every worker (hook + metadata only).
    let cost: Option<Arc<CostGuard>> = match stage {
        "hook" => Some(Arc::new(CostGuard::new(
            cfg.hook.max_tokens_budget,
            cfg.hook.usd_per_1k_tokens,
        ))),
        "metadata" => Some(Arc::new(CostGuard::new(
            cfg.metadata.max_tokens_budget,
            cfg.metadata.usd_per_1k_tokens,
        ))),
        _ => None,
    };

    let queue = Arc::new(Mutex::new(ids.into_iter()));
    let processed = Arc::new(AtomicUsize::new(0));

    // Workers run as concurrent futures on this task (not tokio::spawn), so a
    // worker-owned Connection held across awaits doesn't need to be Sync.
    let workers = (0..max).map(|_| {
        let queue = Arc::clone(&queue);
        let processed = Arc::clone(&processed);
        let client = client.clone();
        let throttle = throttle.clone();
        let cost = cost.clone();
        async move {
            let conn = db::open_and_init(&cfg.db_path)
                .with_context(|| format!("worker opening db {}", cfg.db_path))?;
            loop {
                // Lock only to pop the next id; never held across an await.
                let next = queue.lock().expect("queue mutex poisoned").next();
                let Some(book_id) = next else { break };

                state::mark_running(&conn, book_id, stage)?;
                match dispatch(
                    stage,
                    &conn,
                    cfg,
                    &client,
                    &throttle,
                    cost.as_deref(),
                    book_id,
                )
                .await
                {
                    Ok(()) => {
                        state::mark_done(&conn, book_id, stage)?;
                        processed.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        let msg = format!("{e:#}");
                        eprintln!("    book {book_id} :: {stage} failed: {msg}");
                        state::mark_failed(&conn, book_id, stage, &msg, cfg.max_attempts)?;
                    }
                }
            }
            Ok::<(), anyhow::Error>(())
        }
    });

    // Surface the first worker-level (non-item) error, e.g. a DB open failure.
    for res in futures::future::join_all(workers).await {
        res?;
    }
    Ok(processed.load(Ordering::Relaxed))
}

/// Dispatch one item to its stage module. Network stages receive the shared
/// client + throttle; assemble is local (ffmpeg) and ignores them.
async fn dispatch(
    stage: &str,
    conn: &rusqlite::Connection,
    cfg: &AppConfig,
    client: &Client,
    throttle: &Throttle,
    cost: Option<&CostGuard>,
    book_id: i64,
) -> Result<()> {
    match stage {
        "hook" => {
            let cost = cost.expect("hook stage always has a cost guard");
            hook::run_one(conn, &cfg.hook, client, cost, book_id).await
        }
        "tts" => tts::run_one(conn, &cfg.tts, client, throttle, book_id).await,
        "assemble" => assemble::run_one(conn, &cfg.assemble, book_id).await,
        "metadata" => {
            let cost = cost.expect("metadata stage always has a cost guard");
            metadata::run_one(conn, &cfg.metadata, client, cost, book_id).await
        }
        "thumbnail" => {
            thumbnail::run_one(conn, &cfg.auth, &cfg.thumbnail, client, throttle, book_id).await
        }
        "upload" => upload::run_one(conn, &cfg.auth, &cfg.upload, client, throttle, book_id).await,
        other => anyhow::bail!("runner has no dispatch for stage '{other}'"),
    }
}

/// Selector quota for ingest/hook, capped by the CLI ceiling; else the ceiling.
/// Opens a short-lived connection so callers don't need one in hand.
pub fn produce_limit(cfg: &AppConfig, ceiling: usize) -> usize {
    let conn = match db::open_and_init(&cfg.db_path) {
        Ok(c) => c,
        Err(_) => return ceiling,
    };
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    match selector::quota_for(&conn, &today, &cfg.channel.niche, &cfg.channel.format) {
        Ok(Some(q)) if q > 0 => (q as usize).min(ceiling),
        _ => ceiling,
    }
}
