// src/bin/orchestrator.rs
//
// Top-level driver. Where `pipeline.rs` runs one niche's stages in order,
// the orchestrator runs *many* niches on a schedule, each in its own task,
// with a global concurrency cap so you don't melt your egress or trip
// YouTube's quota across channels.
//
// It does three things and nothing more:
//   1. Discover niche configs (config/*.toml) or take an explicit list.
//   2. For each niche, walk the stage chain via the existing per-niche runner.
//   3. Govern total in-flight work with a shared semaphore + per-niche stagger.
//
// Usage:
//   orchestrator --config-dir config --max-parallel 2
//   orchestrator --niches forgotten_classics,dead_authors --loop --interval-secs 3600
//   orchestrator --config-dir config --stages ingest,hook,tts --no-upload

use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

use youtube_pipeline::{config::AppConfig, db, ingest, runner};

// ============================================================
// Stage enum — same ordering contract as pipeline.rs
// ============================================================

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

    fn full_chain() -> Vec<Stage> {
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
}

impl std::str::FromStr for Stage {
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

// ============================================================
// CLI
// ============================================================

#[derive(Parser, Debug)]
#[command(name = "orchestrator", about = "Multi-niche faceless YouTube driver")]
struct Args {
    /// Directory of per-niche TOML configs. All *.toml are discovered.
    #[arg(long, default_value = "config")]
    config_dir: String,

    /// Explicit comma-separated niche names (file stems) — overrides discovery.
    #[arg(long, value_delimiter = ',')]
    niches: Option<Vec<String>>,

    /// Max niches processed concurrently. Keep low to respect API quota.
    #[arg(long, default_value_t = 2)]
    max_parallel: usize,

    /// Per-stage item cap, passed through to each niche run.
    #[arg(long, default_value_t = 50)]
    limit: usize,

    /// Restrict to a subset of stages, in order.
    #[arg(long, value_delimiter = ',')]
    stages: Option<Vec<Stage>>,

    /// Skip the upload stage everywhere (dry run through assembly + metadata).
    #[arg(long, default_value_t = false)]
    no_upload: bool,

    /// Stagger niche starts by this many ms so they don't all hammer the
    /// same upstream (Gutenberg, OpenAI) in the same instant.
    #[arg(long, default_value_t = 750)]
    stagger_ms: u64,

    /// Run forever, sleeping `interval_secs` between full sweeps.
    #[arg(long, default_value_t = false)]
    r#loop: bool,

    /// Sleep between sweeps when --loop is set.
    #[arg(long, default_value_t = 3600)]
    interval_secs: u64,
}

// ============================================================
// main
// ============================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let plan = resolve_stage_plan(&args);
    let configs = discover_configs(&args).context("resolving niche configs")?;

    if configs.is_empty() {
        anyhow::bail!(
            "no niche configs found in '{}' (and no --niches given)",
            args.config_dir
        );
    }

    println!(
        "=== orchestrator up :: {} niche(s) :: max_parallel={} :: stages=[{}] :: loop={} ===",
        configs.len(),
        args.max_parallel,
        plan.iter()
            .map(|s| s.label())
            .collect::<Vec<_>>()
            .join(" -> "),
        args.r#loop,
    );

    // One sweep, or many. The body is identical — looping just re-runs it.
    loop {
        let sweep_start = std::time::Instant::now();
        run_sweep(&configs, &plan, &args).await;
        println!(
            "=== sweep done in {:.1}s ===",
            sweep_start.elapsed().as_secs_f32()
        );

        if !args.r#loop {
            break;
        }
        println!("--- sleeping {}s until next sweep ---", args.interval_secs);
        tokio::time::sleep(Duration::from_secs(args.interval_secs)).await;
    }

    Ok(())
}

// ============================================================
// sweep — fan niches out under a shared concurrency cap
// ============================================================

/// Spawn one task per niche, each gated by a shared semaphore so no more than
/// `max_parallel` niches touch their stage chains at once. Niche failures are
/// isolated: a panic or hard error in one prints and is dropped, the rest of
/// the sweep continues. The orchestrator never lets a single bad config take
/// down the whole fleet.
async fn run_sweep(configs: &[PathBuf], plan: &[Stage], args: &Args) {
    let sem = Arc::new(Semaphore::new(args.max_parallel.max(1)));

    // Niche futures run concurrently on this task rather than via tokio::spawn:
    // each opens its own rusqlite::Connection, which is Send but not Sync, so a
    // `&Connection` held across an await can't satisfy spawn's Send bound. The
    // shared semaphore still caps how many niches touch their chains at once.
    let niches = configs.iter().cloned().enumerate().map(|(idx, cfg_path)| {
        let sem = Arc::clone(&sem);
        let stagger = Duration::from_millis(args.stagger_ms * idx as u64);
        let limit = args.limit;
        async move {
            // Stagger so all niches don't fire ingest at the exact same tick.
            tokio::time::sleep(stagger).await;

            // Acquire a slot; held until this niche's whole chain finishes.
            let _permit = sem.acquire().await.expect("semaphore closed unexpectedly");

            let niche = cfg_path
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_else(|| "unknown".into());

            match run_niche(&cfg_path, plan, limit).await {
                Ok((ok, failed)) => println!(
                    "[{niche}] sweep complete :: {ok} stage-pass(es) ok, {failed} stage(s) errored"
                ),
                Err(e) => eprintln!("[{niche}] niche aborted: {e:#}"),
            }
        }
    });

    futures::future::join_all(niches).await;
}

// ============================================================
// run_niche — one niche, full stage chain, resumable
// ============================================================

/// Load this niche's config, open its DB, and walk the stage plan in order.
/// A stage erroring is recorded and the chain stops for *this niche only*
/// (downstream stages would run on bad upstream data otherwise). Returns
/// (stages_passed, stages_failed) for the sweep summary.
async fn run_niche(cfg_path: &Path, plan: &[Stage], limit: usize) -> Result<(usize, usize)> {
    let cfg = AppConfig::load(cfg_path.to_str().context("non-utf8 config path")?)
        .with_context(|| format!("loading {}", cfg_path.display()))?;

    {
        // Stamp this DB's niche/format once (used by analytics + selector),
        // then let the runner manage its own per-worker connections.
        let conn = db::open_and_init(&cfg.db_path)
            .with_context(|| format!("opening db {}", cfg.db_path))?;
        db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)
            .context("stamping channel_meta")?;
    }

    let niche = &cfg.channel.name;
    let (mut passed, mut failed) = (0usize, 0usize);

    for &stage in plan {
        let started = std::time::Instant::now();
        println!("[{niche}] -> stage: {}", stage.label());

        match run_stage(stage, &cfg, limit).await {
            Ok(n) => {
                println!(
                    "[{niche}]    {} ok :: {n} item(s) :: {:.1}s",
                    stage.label(),
                    started.elapsed().as_secs_f32()
                );
                passed += 1;
            }
            Err(e) => {
                eprintln!(
                    "[{niche}]    {} FAILED :: {:.1}s :: {e:#}",
                    stage.label(),
                    started.elapsed().as_secs_f32()
                );
                failed += 1;
                // Stop this niche's chain — don't run downstream on bad input.
                break;
            }
        }
    }

    Ok((passed, failed))
}

// ============================================================
// run_stage — delegates to the shared bounded-concurrency runner
// (identical behavior single- or multi-niche)
// ============================================================

async fn run_stage(stage: Stage, cfg: &AppConfig, limit: usize) -> Result<usize> {
    // ingest is the one batch stage — there are no per-book ids before it runs.
    if stage == Stage::Ingest {
        let lim = runner::produce_limit(cfg, limit);
        return ingest::run_batch(&cfg.db_path, &cfg.ingest, lim)
            .await
            .context("ingest stage");
    }

    // ingest + hook produce toward the selector's quota when one exists.
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

// ============================================================
// helpers
// ============================================================

fn resolve_stage_plan(args: &Args) -> Vec<Stage> {
    let mut plan = args.stages.clone().unwrap_or_else(Stage::full_chain);
    if args.no_upload {
        plan.retain(|s| *s != Stage::Upload);
    }
    plan
}

/// Either the explicit --niches list (mapped to config_dir/<name>.toml) or
/// every *.toml in config_dir, sorted for deterministic sweep order.
fn discover_configs(args: &Args) -> Result<Vec<PathBuf>> {
    if let Some(names) = &args.niches {
        let mut out = Vec::with_capacity(names.len());
        for name in names {
            let p = Path::new(&args.config_dir).join(format!("{name}.toml"));
            if !p.exists() {
                anyhow::bail!("niche config not found: {}", p.display());
            }
            out.push(p);
        }
        return Ok(out);
    }

    let mut configs: Vec<PathBuf> = std::fs::read_dir(&args.config_dir)
        .with_context(|| format!("reading config dir {}", args.config_dir))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "toml").unwrap_or(false))
        .collect();

    configs.sort();
    Ok(configs)
}
