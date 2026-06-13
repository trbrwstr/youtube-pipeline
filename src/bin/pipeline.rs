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

use youtube_pipeline::{config::AppConfig, db, ingest, runner, state};

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
#[command(
    name = "pipeline",
    about = "Faceless YouTube pipeline orchestrator + ops CLI"
)]
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
    /// Search the Gutenberg catalog by title/author (read-only).
    Search(SearchArgs),
    /// Ingest specific books by Gutenberg id (curated; bypasses the year filter).
    Add(AddArgs),
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

#[derive(Parser, Debug)]
struct SearchArgs {
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Title/author substring to search the catalog for (case-insensitive).
    query: String,

    /// Max matches to list.
    #[arg(short, long, default_value_t = 50)]
    limit: usize,

    /// Ingest every listed match (capped by --limit) instead of just listing.
    #[arg(long, default_value_t = false)]
    ingest: bool,
}

#[derive(Parser, Debug)]
struct AddArgs {
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Gutenberg ids to ingest, e.g. --ids 1342,98,84
    #[arg(long, value_delimiter = ',')]
    ids: Vec<i64>,

    /// Exact titles to ingest (case-insensitive), e.g. --titles "Frankenstein;Dracula"
    #[arg(long, value_delimiter = ';')]
    titles: Vec<String>,
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
        Command::Search(args) => cmd_search(args).await,
        Command::Add(args) => cmd_add(args).await,
    }
}

/// Load config and ensure the schema/migrations are in place. Every subcommand
/// starts here so a fresh checkout can run any verb without a separate setup.
fn boot(config_path: &str) -> Result<AppConfig> {
    let cfg =
        AppConfig::load(config_path).with_context(|| format!("loading config {config_path}"))?;
    let conn =
        db::open_and_init(&cfg.db_path).context("initializing database / running migrations")?;
    db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)
        .context("stamping channel_meta")?;
    Ok(cfg)
}

// ---------------------------------------------------------------------------
// run
// ---------------------------------------------------------------------------

async fn cmd_run(args: RunArgs) -> Result<()> {
    let cfg = boot(&args.config)?;

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
        plan.iter()
            .map(|s| s.label())
            .collect::<Vec<_>>()
            .join(" -> ")
    );

    for stage in plan {
        let started = std::time::Instant::now();
        println!("\n--- stage: {} ---", stage.label());

        match run_stage(stage, &cfg, args.limit).await {
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

/// Run one stage. Ingest is the batch exception (no per-book id exists before
/// it runs); every other stage goes through the shared bounded-concurrency
/// runner. ingest/hook produce toward the selector's quota when one exists.
async fn run_stage(stage: Stage, cfg: &AppConfig, limit: usize) -> Result<usize> {
    if stage == Stage::Ingest {
        let lim = runner::produce_limit(cfg, limit);
        return ingest::run_batch(&cfg.db_path, &cfg.ingest, lim)
            .await
            .context("ingest stage");
    }

    let lim = if stage == Stage::Hook {
        runner::produce_limit(cfg, limit)
    } else {
        limit
    };
    runner::run_stage(
        cfg,
        stage.label(),
        stage.depends_on().map(|d| d.label()),
        lim,
    )
    .await
}

// ---------------------------------------------------------------------------
// status
// ---------------------------------------------------------------------------

fn cmd_status(args: CommonArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    println!(
        "=== state :: niche='{}' :: db='{}' ===",
        cfg.channel.name, cfg.db_path
    );
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

// ---------------------------------------------------------------------------
// search / add — curate specific titles from the Gutenberg catalog
// ---------------------------------------------------------------------------

async fn cmd_search(args: SearchArgs) -> Result<()> {
    let cfg = boot(&args.config)?;
    let conn = db::open_and_init(&cfg.db_path)?;

    let matches = ingest::search_catalog(&cfg.ingest, &args.query, args.limit)
        .await
        .context("searching catalog")?;

    if matches.is_empty() {
        println!("no catalog matches for '{}'", args.query);
        return Ok(());
    }

    println!(
        "=== {} match(es) for '{}' (showing up to {}) ===",
        matches.len(),
        args.query,
        args.limit
    );
    let (id_h, year_h, lang_h, title_h, author_h) = ("id", "year", "lang", "title", "author");
    println!("{id_h:>8}  {year_h:>4}  {lang_h:<4}  {title_h:<45}  {author_h}");
    for m in &matches {
        let in_lib = ingest::book_exists(&conn, m.gutenberg_id).unwrap_or(false);
        let year = m
            .issued_year
            .map(|y| y.to_string())
            .unwrap_or_else(|| "----".into());
        let title: String = m.title.chars().take(45).collect();
        println!(
            "{:>8}  {:>4}  {:<4}  {:<45}  {}{}",
            m.gutenberg_id,
            year,
            m.language,
            title,
            m.author,
            if in_lib { "  [in library]" } else { "" },
        );
    }
    if args.ingest {
        let ids: Vec<i64> = matches.iter().map(|m| m.gutenberg_id).collect();
        let n = ingest::ingest_ids(&cfg.db_path, &cfg.ingest, &ids)
            .await
            .context("ingesting search matches")?;
        println!(
            "\ningested {n} of {} match(es) into '{}'.",
            ids.len(),
            cfg.channel.name
        );
    } else {
        println!(
            "\nIngest these:  pipeline add --config {} --ids <id,id,...>   \
             (or re-run search with --ingest)",
            args.config
        );
    }
    Ok(())
}

async fn cmd_add(args: AddArgs) -> Result<()> {
    if args.ids.is_empty() && args.titles.is_empty() {
        anyhow::bail!("pass --ids and/or --titles");
    }
    let cfg = boot(&args.config)?;

    let mut added = 0usize;
    if !args.ids.is_empty() {
        added += ingest::ingest_ids(&cfg.db_path, &cfg.ingest, &args.ids)
            .await
            .context("ingesting requested ids")?;
    }
    if !args.titles.is_empty() {
        added += ingest::ingest_titles(&cfg.db_path, &cfg.ingest, &args.titles)
            .await
            .context("ingesting requested titles")?;
    }

    println!(
        "added {added} book(s) to '{}'. Produce them with: \
         pipeline run --config {} --stages hook,tts,assemble,metadata",
        cfg.channel.name, args.config
    );
    Ok(())
}
