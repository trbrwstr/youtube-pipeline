// src/bin/selector.rs
//
// Score niches by revenue-per-video and write the next cycle's production_plan.
// ingest/hook then read their batch size from quota_for(). Run after analytics.

use anyhow::{Context, Result};
use clap::Parser;

use youtube_pipeline::{config::AppConfig, db, selector};

#[derive(Parser, Debug)]
#[command(name = "selector", about = "Reallocate production quota into production_plan")]
struct Args {
    /// Per-niche TOML config.
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// Plan date (ISO YYYY-MM-DD). Defaults to today.
    #[arg(long)]
    run_date: Option<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let cfg = AppConfig::load(&args.config)
        .with_context(|| format!("loading config {}", args.config))?;
    let conn = db::open_and_init(&cfg.db_path)
        .with_context(|| format!("opening db {}", cfg.db_path))?;
    db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;

    let run_date = args
        .run_date
        .unwrap_or_else(|| chrono::Utc::now().format("%Y-%m-%d").to_string());

    let n = selector::run(&conn, &cfg.selector, &run_date)?;
    println!("selector: wrote {n} plan row(s) for {run_date}");
    Ok(())
}
