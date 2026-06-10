// src/bin/analytics.rs
//
// Pull a fresh performance snapshot for every published video into video_stats.
// Run it on its own cadence (daily is plenty) between production sweeps.

use anyhow::{Context, Result};
use clap::Parser;

use youtube_pipeline::{analytics, config::AppConfig, db};

#[derive(Parser, Debug)]
#[command(name = "analytics", about = "Pull YouTube Analytics into video_stats")]
struct Args {
    /// Per-niche TOML config.
    #[arg(short, long, default_value = "config/forgotten_classics.toml")]
    config: String,

    /// End the reporting window this many days before today. Revenue settles
    /// ~2 days late, so the default avoids logging phantom $0.00s as truth.
    #[arg(long, default_value_t = 2)]
    until_days_ago: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let cfg =
        AppConfig::load(&args.config).with_context(|| format!("loading config {}", args.config))?;
    let conn =
        db::open_and_init(&cfg.db_path).with_context(|| format!("opening db {}", cfg.db_path))?;
    db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;

    let n = analytics::run(&conn, &cfg, args.until_days_ago).await?;
    println!("analytics: recorded {n} video snapshot(s)");
    Ok(())
}
