// src/bin/pipeline.rs
//
// End-to-end orchestrator + operational CLI for the faceless YouTube pipeline.
//
//   run      ingest -> hook -> tts -> assemble -> metadata -> thumbnail -> upload
//   status   per-stage counts (pending / running / done / failed)
//   reap     reset stale "running" rows whose worker died mid-stage
//   retry    push failed rows back to pending for a given stage
//   dead     list dead-letter rows (failed past max attempts)
//
// Every stage is idempotent and state-gated via state.rs, so Ctrl-C anywhere
// and a re-run picks up exactly where it stopped. Nothing downstream runs on
// bad upstream output.
//
// Usage:
//   pipeline run    --config config/forgotten_classics.toml --limit 50
//   pipeline run    --config config/forgotten_classics.toml --stages hook,tts
//   pipeline status --config config/forgotten_classics.toml
//   pipeline reap   --config config/forgotten_classics.toml --stale-secs 900
//   pipeline retry  --config config/forgotten_classics.toml --stage upload
//   pipeline dead   --config config/forgotten_classics.toml

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::str::FromStr;

use youtube_pipeline::{
    assemble, config::AppConfig, db, hook, ingest, metadata, selector, state, thumbnail, tts,
    upload,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Ingest,
    Hook,
    Tts,
    Assemble,
    Metadata,
    Thumbnail,
    Upload,
}

impl Stage {
    /// Full pipeline order — drives the default `run`.
    fn all() -> Vec<Stage> {
        vec![
            Stage::Ingest,
            Stage::Hook,
            Stage::Tts,
            Stage::Assemble,
            Stage::Metadata,
            Stage::Thumbnail,
            Stage::Upload,
        ]
    }

    fn label(&self) -> &'static str {
        match self {
            Stage::Ingest => "ingest",
            Stage::Hook => "hook",
            Stage::Tts => "tts",
            Stage::Assemble => "assemble",
            Stage::Metadata => "metadata",
            Stage::Thumbnail => "thumbnail",
            Stage::Upload => "upload",
        }
    }

    /// Upstream stage that must be `done` before this one is eligible.
    fn depends_on(&self) -> Option<Stage> {
        match self {
            Stage::Ingest => None,
            Stage::Hook => Some(Stage::Ingest),
            Stage::Tts => Some(Stage::Hook),
            Stage::Assemble => Some(Stage::Tts),
            Stage::Metadata => Some(Stage::Assemble),
            Stage::Thumbnail => Some(Stage::Metadata),
            Stage::Upload => Some(Stage::Thumbnail),
        }
    }
}

impl FromStr for Stage {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self> {
        Ok(match s.trim().to_ascii_lowercase().as_str() {
            "ingest" => Stage::Ingest,
            "hook" => Stage::Hook,
            "tts" => Stage::Tts,
            "assemble" => Stage::Assemble,
            "metadata" => Stage::Metadata,
            "thumbnail" => Stage::Thumbnail,
            "upload" => Stage::Upload,
            other => anyhow::bail!("unknown stage '{other}'"),
        })
    }
}

#[derive(Parser, Debug)]
#[command(name = "pipeline", about = "Faceless YouTube pipeline orchestrator + ops CLI")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run the pipeline (full chain or a stage subset).
    Run(RunArgs),
    /// Print per-stage state counts and exit.
    Status(CommonArgs),
    /// Reset stale `running` rows back to `pending` so a re-run reclaims them.
    Reap(ReapArgs),
    /// Push `failed` rows for a stage back to `pending`.
    Retry(RetryArgs),
    /// List dead-letter rows (failed past max attempts).
    Dead(CommonArgs),
}

#[derive(Parser, Debug)]
struct CommonArgs {
    /// Path to the per-niche TOML config.
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,
}

#[derive(Parser, Debug)]
struct RunArgs {
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Max items to process per stage this run.
    #[arg(short, long, default_value_t = 50)]
    limit: usize,

    /// Run a single stage only (mutually exclusive with --stages).
    #[arg(long)]
    stage: Option<Stage>,

    /// Run a comma-separated subset, in the given order.
    #[arg(long, value_delimiter = ',')]
    stages: Option<Vec<Stage>>,

    /// Skip the actual YouTube upload (dry run through assembly + metadata).
    #[arg(long, default_value_t = false)]
    no_upload: bool,
}

#[derive(Parser, Debug)]
struct ReapArgs {
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// A `running` row older than this many seconds is presumed dead.
    #[arg(long, default_value_t = 900)]
    stale_secs: i64,
}

#[derive(Parser, Debug)]
struct RetryArgs {
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Which stage's failed rows to revive.
    #[arg(long)]
    stage: Stage,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Run(args) => cmd_run(args).await,
        Command::Status(args) => cmd_status(args),
        Command::Reap(args) => cmd_reap(args),
        Command::Retry(args) => cmd_retry(args),
        Command::Dead(args) => cmd_dead(args),
    }
}

/// Load config and ensure the schema/migrations are in place. Every subcommand
/// starts here so a fresh checkout can run any verb without a separate setup.
fn boot(config_path: &str) -> Result<AppConfig> {
    let cfg = AppConfig::load(config_path)
        .with_context(|| format!("loading config {config_path}"))?;
    let conn = db::open_and_init(&cfg.db_path)
        .context("initializing database / running migrations")?;
    db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)
        .context("stamping channel_meta")?;
    Ok(cfg)
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

async fn cmd_run(args: RunArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)
        .with_context(|| format!("opening db {}", cfg.db_path))?;

    // Resolve which stages to run.
    let plan: Vec<Stage> = match (args.stage, args.stages.clone()) {
        (Some(_), Some(_)) => anyhow::bail!("pass either --stage or --stages, not both"),
        (Some(one), None) => vec![one],
        (None, Some(many)) => many,
        (None, None) => {
            let mut all = Stage::all();
            if args.no_upload {
                all.retain(|s| *s != Stage::Upload);
            }
            all
        }
    };

    println!(
        "=== pipeline run :: niche='{}' :: limit={} :: stages=[{}] ===",
        cfg.channel.name,
        args.limit,
        plan.iter().map(|s| s.label()).collect::<Vec<_>>().join(" -> ")
    );

    for stage in plan {
        let started = std::time::Instant::now();
        println!("\n--- stage: {} ---", stage.label());

        match run_stage(&conn, stage, &cfg, args.limit).await {
            Ok(n) => println!(
                "--- {} done :: {} item(s) :: {:.1}s ---",
                stage.label(),
                n,
                started.elapsed().as_secs_f32()
            ),
            Err(e) => {
                eprintln!(
                    "!!! {} FAILED after {:.1}s: {e:#}",
                    stage.label(),
                    started.elapsed().as_secs_f32()
                );
                // Hard short-circuit: don't run a stage against bad upstream
                // output. Failed rows are already marked in state.rs, so a
                // re-run (or `retry`) resumes cleanly once the cause is fixed.
                return Err(e).with_context(|| format!("stage '{}' failed", stage.label()));
            }
        }
    }

    println!("\n=== pipeline run complete ===");
    Ok(())
}

/// Run one stage over its eligible items via the shared state machine. Ingest
/// is the batch exception (no per-book id exists before it runs); every other
/// stage claims items, runs `run_one`, and stamps terminal state. Returns the
/// count of items processed this run (0 = nothing left to do — not an error).
async fn run_stage(
    conn: &rusqlite::Connection,
    stage: Stage,
    cfg: &AppConfig,
    limit: usize,
) -> Result<usize> {
    if stage == Stage::Ingest {
        return ingest::run_batch(&cfg.db_path, &cfg.ingest, produce_limit(conn, cfg, limit))
            .await
            .context("ingest stage");
    }

    let effective_limit = if stage == Stage::Hook {
        produce_limit(conn, cfg, limit)
    } else {
        limit
    };

    let book_ids = state::eligible_for_stage(
        conn,
        stage.label(),
        stage.depends_on().map(|d| d.label()),
        cfg.max_attempts,
        effective_limit,
    )
    .with_context(|| format!("selecting eligible items for {}", stage.label()))?;

    let mut processed = 0usize;
    for book_id in book_ids {
        state::mark_running(conn, book_id, stage.label())?;
        match run_one(stage, conn, cfg, book_id).await {
            Ok(()) => {
                state::mark_done(conn, book_id, stage.label())?;
                processed += 1;
            }
            Err(e) => {
                let msg = format!("{e:#}");
                eprintln!("    book {book_id} :: {} failed: {msg}", stage.label());
                state::mark_failed(conn, book_id, stage.label(), &msg, cfg.max_attempts)?;
            }
        }
    }
    Ok(processed)
}

/// Dispatch one item to its stage module. Mirrors orchestrator.rs so single-
/// and multi-niche runs share identical per-item behavior.
async fn run_one(
    stage: Stage,
    conn: &rusqlite::Connection,
    cfg: &AppConfig,
    book_id: i64,
) -> Result<()> {
    match stage {
        Stage::Ingest => unreachable!("ingest is handled as a batch in run_stage"),
        Stage::Hook => hook::run_one(conn, &cfg.hook, book_id).await,
        Stage::Tts => tts::run_one(conn, &cfg.tts, book_id).await,
        Stage::Assemble => assemble::run_one(conn, &cfg.assemble, book_id).await,
        Stage::Metadata => metadata::run_one(conn, &cfg.metadata, book_id).await,
        Stage::Thumbnail => thumbnail::run_one(conn, &cfg.auth, &cfg.thumbnail, book_id).await,
        Stage::Upload => upload::run_one(conn, &cfg.auth, &cfg.upload, book_id).await,
    }
}

/// Selector quota for ingest/hook, capped by the CLI ceiling; else the ceiling.
fn produce_limit(conn: &rusqlite::Connection, cfg: &AppConfig, ceiling: usize) -> usize {
    let today = chrono::Utc::now().format("%Y-%m-%d").to_string();
    match selector::quota_for(conn, &today, &cfg.channel.niche, &cfg.channel.format) {
        Ok(Some(q)) if q > 0 => (q as usize).min(ceiling),
        _ => ceiling,
    }
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

fn cmd_status(args: CommonArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    println!("=== state :: niche='{}' :: db='{}' ===", cfg.channel.name, cfg.db_path);
    println!(
        "{:<10} {:>8} {:>8} {:>8} {:>8}",
        "stage", "pending", "running", "done", "failed"
    );
    println!("{}", "-".repeat(46));

    for stage in Stage::all() {
        let c = state::stage_counts_for(&conn, stage.label())
            .with_context(|| format!("counting state for {}", stage.label()))?;
        println!(
            "{:<10} {:>8} {:>8} {:>8} {:>8}",
            stage.label(),
            c.pending,
            c.running,
            c.done,
            c.failed
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// reap
// ---------------------------------------------------------------------------

fn cmd_reap(args: ReapArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    let reaped = state::reap_stale(&conn, args.stale_secs, cfg.max_attempts)
        .context("reaping stale running rows")?;

    println!(
        "reaped {reaped} stale row(s) older than {}s back to 'pending'",
        args.stale_secs
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// retry
// ---------------------------------------------------------------------------

fn cmd_retry(args: RetryArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    let revived = state::retry_stage(&conn, args.stage.label())
        .with_context(|| format!("retrying failed rows for {}", args.stage.label()))?;

    println!(
        "moved {revived} failed '{}' row(s) back to 'pending'",
        args.stage.label()
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// dead
// ---------------------------------------------------------------------------

fn cmd_dead(args: CommonArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    let rows = state::dead_letters(&conn, None, 200).context("listing dead-letter rows")?;

    if rows.is_empty() {
        println!("no dead-letter rows — nothing failed past max attempts");
        return Ok(());
    }

    println!("=== dead letters :: {} row(s) ===", rows.len());
    for d in rows {
        println!(
            "book_id={:<6} stage={:<10} attempts={:<3} last_error={}",
            d.book_id, d.stage, d.attempts, d.last_error
        );
    }
    Ok(())
}