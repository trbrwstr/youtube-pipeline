// src/bin/selector.rs
//
// Score niches by revenue-per-video and write the next cycle's production_plan.
// ingest/hook then read their batch size from quota_for(). Run after analytics.
//
// Two modes:
//   --config <file>     single-niche: allocate this niche's budget alone.
//   --config-dir <dir>  federated: read every niche's video_stats, allocate ONE
//                       shared budget across all of them, and write each niche's
//                       quota back into its own DB. This is the cross-channel
//                       explore/exploit reallocation — winners pull budget from
//                       losers across the whole fleet.

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use rusqlite::Connection;
use std::collections::BTreeMap;
use std::path::PathBuf;

use youtube_pipeline::config::{AppConfig, SelectorConfig};
use youtube_pipeline::db;
use youtube_pipeline::selector::{self, NicheKey};

#[derive(Parser, Debug)]
#[command(
    name = "selector",
    about = "Reallocate production quota into production_plan"
)]
struct Args {
    /// Single niche TOML config. Ignored when --config-dir is given.
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Directory of niche configs. Allocate ONE shared budget across every
    /// niche found here (federated cross-niche mode).
    #[arg(long)]
    config_dir: Option<String>,

    /// Plan date (ISO YYYY-MM-DD). Defaults to today.
    #[arg(long)]
    run_date: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let run_date = args
        .run_date
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    let n = match &args.config_dir {
        Some(dir) => run_federated(dir, &run_date)?,
        None => run_single(&args.config, &run_date)?,
    };
    println!("selector: wrote {n} plan row(s) for {run_date}");
    Ok(())
}

/// One niche, its own budget. Backward-compatible default.
fn run_single(config: &str, run_date: &str) -> Result<usize> {
    let cfg = AppConfig::load(config).with_context(|| format!("loading config {config}"))?;
    let conn =
        db::open_and_init(&cfg.db_path).with_context(|| format!("opening db {}", cfg.db_path))?;
    db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;
    selector::run(&conn, &cfg.selector, run_date)
}

/// Federated: pool every niche's performance into one allocation, then write
/// each niche's share back to its own DB so quota_for() picks it up locally.
fn run_federated(config_dir: &str, run_date: &str) -> Result<usize> {
    let paths = discover_configs(config_dir)?;
    if paths.is_empty() {
        bail!("no .toml configs found in {config_dir}");
    }

    let mut scores: BTreeMap<NicheKey, f64> = BTreeMap::new();
    let mut samples: BTreeMap<NicheKey, i64> = BTreeMap::new();
    // Where each niche's allocated row gets written (its own DB).
    let mut routes: Vec<(NicheKey, Connection)> = Vec::new();
    // The shared budget/policy. All niches should carry an identical [selector]
    // block in federated mode; we adopt the first one and report it.
    let mut policy: Option<SelectorConfig> = None;
    let mut policy_src = String::new();

    for path in &paths {
        let path_str = path.to_str().context("non-utf8 config path")?;
        let cfg = AppConfig::load(path_str).with_context(|| format!("loading {path_str}"))?;
        let conn = db::open_and_init(&cfg.db_path)
            .with_context(|| format!("opening db {}", cfg.db_path))?;
        db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;

        // Merge this niche's measured performance into the global maps.
        let (s, n) = selector::score_niches(&conn)?;
        scores.extend(s);
        samples.extend(n);

        // Ensure the niche appears even with zero published videos so it still
        // earns a baseline (explore-track) allocation.
        let key = (cfg.channel.niche.clone(), cfg.channel.format.clone());
        scores.entry(key.clone()).or_insert(0.0);
        samples.entry(key.clone()).or_insert(0);

        if policy.is_none() {
            policy = Some(cfg.selector.clone());
            policy_src = key.0.clone();
        }
        routes.push((key, conn));
    }

    let policy = policy.ok_or_else(|| anyhow!("no usable niche policy"))?;
    println!(
        "selector: federated over {} niche(s) :: budget={} from '{}'",
        routes.len(),
        policy.total_budget,
        policy_src
    );

    let plan = selector::allocate(&scores, &samples, &policy);

    let mut wrote = 0usize;
    for row in &plan {
        let (niche, format, quota, reason) = row;
        if let Some((_, conn)) = routes.iter().find(|(k, _)| &k.0 == niche && &k.1 == format) {
            selector::write_plan(conn, run_date, std::slice::from_ref(row))?;
            println!("  {niche}/{format}: quota={quota} ({reason})");
            wrote += 1;
        }
    }
    Ok(wrote)
}

/// Every *.toml in `dir`, sorted for deterministic order.
fn discover_configs(dir: &str) -> Result<Vec<PathBuf>> {
    let mut configs: Vec<PathBuf> = std::fs::read_dir(dir)
        .with_context(|| format!("reading config dir {dir}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p: &PathBuf| p.extension().map(|x| x == "toml").unwrap_or(false))
        .collect();
    configs.sort();
    Ok(configs)
}
