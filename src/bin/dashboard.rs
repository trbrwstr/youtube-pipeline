// src/bin/dashboard.rs
//
// Web dashboard for the YouTube pipeline. Serves a single-page UI and a REST
// API over every niche's data and operations.
//
// Usage:
//   cargo run --bin dashboard -- --port 3000 --config-dir config
//
// Then open http://localhost:3000

use anyhow::{bail, Context, Result};
use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use youtube_pipeline::{
    analytics, config::AppConfig, db, ingest, runner, selector, state,
};

// ============================================================
//  Stage model
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Ingest, Hook, Tts, Assemble, Metadata, Thumbnail, Upload,
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
    fn all() -> Vec<Stage> {
        vec![
            Stage::Ingest, Stage::Hook, Stage::Tts, Stage::Assemble,
            Stage::Metadata, Stage::Thumbnail, Stage::Upload,
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
//  Shared state
// ============================================================

#[derive(Debug, Clone, Serialize)]
struct Job {
    id: String,
    niche: String,
    kind: String,
    status: String,
    message: String,
}

#[derive(Default)]
struct AppState {
    config_dir: String,
    jobs: RwLock<HashMap<String, Job>>,
}

// ============================================================
//  CLI + main
// ============================================================

#[derive(Parser, Debug)]
#[command(name = "dashboard", about = "Web dashboard for the YouTube pipeline")]
struct Args {
    #[arg(long, default_value = "3000")]
    port: u16,
    #[arg(long, default_value = "config")]
    config_dir: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "dashboard=debug,tower_http=debug".into()))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Args::parse();
    let state = Arc::new(AppState {
        config_dir: args.config_dir.clone(),
        jobs: RwLock::new(HashMap::new()),
    });

    let app = Router::new()
        .route("/", get(index_handler))
        .route("/api/niches", get(list_niches))
        .route("/api/niches/:niche/status", get(niche_status))
        .route("/api/niches/:niche/run", post(run_niche))
        .route("/api/niches/:niche/reap", post(reap_niche))
        .route("/api/niches/:niche/retry", post(retry_niche))
        .route("/api/niches/:niche/dead", get(dead_niche))
        .route("/api/niches/:niche/books", get(list_books))
        .route("/api/niches/:niche/frames", get(list_frames))
        .route("/api/niches/:niche/ingest", post(ingest_books))
        .route("/api/niches/:niche/plan", get(production_plan))
        .route("/api/niches/:niche/stats", get(video_stats))
        .route("/api/niches/:niche/analytics", post(run_analytics))
        .route("/api/orchestrator/run", post(run_orchestrator))
        .route("/api/selector/run", post(run_selector))
        .route("/api/jobs", get(list_jobs))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", args.port)).await?;
    tracing::info!("dashboard listening on http://0.0.0.0:{}", args.port);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index_handler() -> Html<&'static str> {
    Html(INDEX_HTML)
}

// ============================================================
//  Helpers
// ============================================================

fn discover_configs(config_dir: &str) -> Result<Vec<PathBuf>> {
    let mut configs: Vec<PathBuf> = std::fs::read_dir(config_dir)
        .with_context(|| format!("reading config dir {config_dir}"))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "toml").unwrap_or(false))
        .collect();
    configs.sort();
    Ok(configs)
}

fn niche_path(config_dir: &str, niche: &str) -> Result<PathBuf> {
    let p = std::path::Path::new(config_dir).join(format!("{niche}.toml"));
    if !p.exists() { bail!("niche config not found: {}", p.display()); }
    Ok(p)
}

fn load_config(path: &std::path::Path) -> Result<AppConfig> {
    let s = path.to_str().context("non-utf8 config path")?;
    AppConfig::load(s)
}

async fn spawn_job<F>(state: &Arc<AppState>, niche: &str, kind: &str, f: F)
where F: FnOnce() -> Result<String> + Send + 'static,
{
    let id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    let job = Job {
        id: id.clone(), niche: niche.to_string(), kind: kind.to_string(),
        status: "running".into(), message: "started".into(),
    };
    state.jobs.write().await.insert(id.clone(), job);
    let state2 = Arc::clone(state);
    tokio::task::spawn_blocking(move || {
        let msg = match f() {
            Ok(m) => m,
            Err(e) => format!("error: {e:#}"),
        };
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            if let Some(j) = state2.jobs.write().await.get_mut(&id) {
                j.status = "done".into(); j.message = msg;
            }
        });
    });
}

// ============================================================
//  API: niches + status
// ============================================================

#[derive(Serialize)]
struct NichesResponse { niches: Vec<NicheSummary> }

#[derive(Serialize)]
struct NicheSummary {
    name: String, niche: String, format: String,
    config_path: String, db_path: String, stages: Vec<StageSummary>,
}

#[derive(Serialize)]
struct StageSummary {
    stage: String, pending: i64, running: i64,
    done: i64, failed: i64, dead: i64,
}

async fn list_niches(State(state): State<Arc<AppState>>)
-> Result<Json<NichesResponse>, StatusCode> {
    let configs = discover_configs(&state.config_dir).map_err(|e| {
        tracing::error!("discover configs: {e:#}"); StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut niches = Vec::with_capacity(configs.len());
    for path in configs {
        let _niche_name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown").to_string();
        let cfg = match load_config(&path) {
            Ok(c) => c,
            Err(e) => { tracing::warn!("skip {}: {e:#}", path.display()); continue; }
        };
        let stages = match db::open_and_init(&cfg.db_path) {
            Ok(conn) => Stage::all().iter().map(|stage| {
                let c = state::stage_counts_for(&conn, stage.label()).unwrap_or_default();
                StageSummary {
                    stage: stage.label().to_string(), pending: c.pending, running: c.running,
                    done: c.done, failed: c.failed, dead: c.dead,
                }
            }).collect(),
            Err(_) => Vec::new(),
        };
        niches.push(NicheSummary {
            name: cfg.channel.name, niche: cfg.channel.niche, format: cfg.channel.format,
            config_path: path.to_string_lossy().into_owned(), db_path: cfg.db_path, stages,
        });
    }
    Ok(Json(NichesResponse { niches }))
}

async fn niche_status(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<Vec<StageSummary>>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let out = Stage::all().iter().map(|stage| {
        let c = state::stage_counts_for(&conn, stage.label()).unwrap_or_default();
        StageSummary {
            stage: stage.label().to_string(), pending: c.pending, running: c.running,
            done: c.done, failed: c.failed, dead: c.dead,
        }
    }).collect();
    Ok(Json(out))
}

// ============================================================
//  API: run / reap / retry / dead
// ============================================================

#[derive(Deserialize)]
struct RunRequest {
    stages: Option<Vec<String>>, limit: Option<usize>,
    #[serde(default)] no_upload: bool,
}

#[derive(Serialize)]
struct ActionResponse { job_id: String, status: String }

async fn run_one_stage(stage: Stage, cfg: &AppConfig, limit: usize) -> Result<usize> {
    if stage == Stage::Ingest {
        let lim = runner::produce_limit(cfg, limit);
        return ingest::run_batch(&cfg.db_path, &cfg.ingest, lim).await.context("ingest stage");
    }
    let lim = if stage == Stage::Hook { runner::produce_limit(cfg, limit) } else { limit };
    runner::run_stage(cfg, stage.label(), stage.depends_on().map(|d| d.label()), lim)
        .await.with_context(|| format!("{} stage", stage.label()))
}

async fn run_niche(State(state): State<Arc<AppState>>, Path(niche): Path<String>, Json(req): Json<RunRequest>)
-> Result<Json<ActionResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let limit = req.limit.unwrap_or(50);
    let mut plan: Vec<Stage> = match req.stages {
        Some(s) => s.iter().map(|x| x.parse()).collect::<Result<_>>().map_err(|_| StatusCode::BAD_REQUEST)?,
        None => Stage::all(),
    };
    if req.no_upload { plan.retain(|s| *s != Stage::Upload); }

    let job_id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    let job = Job {
        id: job_id.clone(), niche: niche.clone(), kind: "run".into(),
        status: "running".into(),
        message: format!("stages: {}", plan.iter().map(|s| s.label()).collect::<Vec<_>>().join(", ")),
    };
    state.jobs.write().await.insert(job_id.clone(), job);

    let state2 = Arc::clone(&state);
    let id2 = job_id.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut ok = 0usize;
            for stage in plan {
                match run_one_stage(stage, &cfg, limit).await {
                    Result::Ok(n) => { tracing::info!("[{niche}] {} done :: {n} items", stage.label()); ok += 1; }
                    Result::Err(e) => { tracing::error!("[{niche}] {} failed: {e:#}", stage.label()); break; }
                }
            }
            let msg = format!("{ok} stage(s) completed");
            if let Some(j) = state2.jobs.write().await.get_mut(&id2) {
                j.status = "done".into(); j.message = msg;
            }
        });
    });
    Ok(Json(ActionResponse { job_id, status: "running".into() }))
}

#[derive(Deserialize)]
struct ReapRequest { stale_secs: Option<i64> }

async fn reap_niche(State(state): State<Arc<AppState>>, Path(niche): Path<String>, Json(req): Json<ReapRequest>)
-> Result<Json<ActionResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let stale = req.stale_secs.unwrap_or(900);
    spawn_job(&state, &niche, "reap", move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let conn = db::open_and_init(&cfg.db_path)?;
            let n = state::reap_stale(&conn, stale, cfg.max_attempts)?;
            Ok(format!("reaped {n} stale row(s)"))
        })
    }).await;
    let id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    Ok(Json(ActionResponse { job_id: id, status: "running".into() }))
}

#[derive(Deserialize)]
struct RetryRequest { stage: String }

async fn retry_niche(State(state): State<Arc<AppState>>, Path(niche): Path<String>, Json(req): Json<RetryRequest>)
-> Result<Json<ActionResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let stage = req.stage;
    spawn_job(&state, &niche, "retry", move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let conn = db::open_and_init(&cfg.db_path)?;
            let n = state::retry_stage(&conn, &stage)?;
            Ok(format!("revived {n} failed row(s) for {stage}"))
        })
    }).await;
    let id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    Ok(Json(ActionResponse { job_id: id, status: "running".into() }))
}

#[derive(Serialize)]
struct DeadResponse { rows: Vec<DeadRowJson> }
#[derive(Serialize)]
struct DeadRowJson { book_id: i64, stage: String, attempts: i64, last_error: String }

async fn dead_niche(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<DeadResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = state::dead_letters(&conn, None, 200).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let out = rows.into_iter().map(|r| DeadRowJson {
        book_id: r.book_id, stage: r.stage, attempts: r.attempts, last_error: r.last_error,
    }).collect();
    Ok(Json(DeadResponse { rows: out }))
}

// ============================================================
//  API: books / frames / ingest
// ============================================================

#[derive(Serialize)]
struct BooksResponse { books: Vec<BookJson> }
#[derive(Serialize)]
struct BookJson { id: i64, gutenberg_id: i64, title: String, author: String, language: String, issued_year: Option<i32> }

async fn list_books(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<BooksResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn.prepare("SELECT id, gutenberg_id, title, author, language, issued_year FROM books ORDER BY id DESC LIMIT 200")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = stmt.query_map([], |r| Ok(BookJson {
        id: r.get(0)?, gutenberg_id: r.get(1)?, title: r.get(2)?,
        author: r.get(3)?, language: r.get(4)?, issued_year: r.get(5)?,
    })).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let books: Vec<BookJson> = rows.collect::<Result<_, _>>().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(BooksResponse { books }))
}

#[derive(Serialize)]
struct FramesResponse { frames: Vec<FrameJson> }
#[derive(Serialize)]
struct FrameJson { book_id: i64, hook_text: Option<String>, audio_path: Option<String>, output_path: Option<String>, youtube_id: Option<String> }

async fn list_frames(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<FramesResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn.prepare("SELECT book_id, hook_text, audio_path, output_path, youtube_id FROM script_frames ORDER BY book_id DESC LIMIT 200")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = stmt.query_map([], |r| Ok(FrameJson {
        book_id: r.get(0)?, hook_text: r.get(1)?, audio_path: r.get(2)?,
        output_path: r.get(3)?, youtube_id: r.get(4)?,
    })).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let frames: Vec<FrameJson> = rows.collect::<Result<_, _>>().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(FramesResponse { frames }))
}

#[derive(Deserialize)]
struct IngestRequest { ids: Option<Vec<i64>>, titles: Option<Vec<String>> }

async fn ingest_books(State(state): State<Arc<AppState>>, Path(niche): Path<String>, Json(req): Json<IngestRequest>)
-> Result<Json<ActionResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    spawn_job(&state, &niche, "ingest", move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let mut added = 0usize;
            if let Some(ids) = req.ids { added += ingest::ingest_ids(&cfg.db_path, &cfg.ingest, &ids).await?; }
            if let Some(titles) = req.titles { added += ingest::ingest_titles(&cfg.db_path, &cfg.ingest, &titles).await?; }
            Ok(format!("added {added} book(s)"))
        })
    }).await;
    let id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    Ok(Json(ActionResponse { job_id: id, status: "running".into() }))
}

// ============================================================
//  API: plan / stats / analytics / orchestrator / selector / jobs
// ============================================================

#[derive(Serialize)]
struct PlanResponse { plans: Vec<PlanRow> }
#[derive(Serialize)]
struct PlanRow { run_date: String, niche: String, format: String, quota: i64, reason: String }

async fn production_plan(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<PlanResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn.prepare("SELECT run_date, niche, format, quota, reason FROM production_plan ORDER BY run_date DESC LIMIT 30")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = stmt.query_map([], |r| Ok(PlanRow {
        run_date: r.get(0)?, niche: r.get(1)?, format: r.get(2)?,
        quota: r.get(3)?, reason: r.get(4)?,
    })).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let plans: Vec<PlanRow> = rows.collect::<Result<_, _>>().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(PlanResponse { plans }))
}

#[derive(Serialize)]
struct StatsResponse { stats: Vec<StatRow> }
#[derive(Serialize)]
struct StatRow { video_id: String, snapshot_date: String, views: i64, est_revenue_usd: f64 }

async fn video_stats(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<StatsResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    let conn = db::open_and_init(&cfg.db_path).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut stmt = conn.prepare("SELECT video_id, snapshot_date, views, est_revenue_usd FROM video_stats ORDER BY snapshot_date DESC, video_id DESC LIMIT 200")
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = stmt.query_map([], |r| Ok(StatRow {
        video_id: r.get(0)?, snapshot_date: r.get(1)?,
        views: r.get(2)?, est_revenue_usd: r.get(3)?,
    })).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stats: Vec<StatRow> = rows.collect::<Result<_, _>>().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(StatsResponse { stats }))
}

async fn run_analytics(State(state): State<Arc<AppState>>, Path(niche): Path<String>)
-> Result<Json<ActionResponse>, StatusCode> {
    let path = niche_path(&state.config_dir, &niche).map_err(|_| StatusCode::NOT_FOUND)?;
    let cfg = load_config(&path).map_err(|_| StatusCode::NOT_FOUND)?;
    spawn_job(&state, &niche, "analytics", move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let conn = db::open_and_init(&cfg.db_path)?;
            db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;
            let n = analytics::run(&conn, &cfg, 2).await?;
            Ok(format!("recorded {n} snapshot(s)"))
        })
    }).await;
    let id = format!("{}-{}", niche, chrono::Utc::now().timestamp_millis());
    Ok(Json(ActionResponse { job_id: id, status: "running".into() }))
}

#[derive(Deserialize)]
struct OrchestratorRequest {
    stages: Option<Vec<String>>, limit: Option<usize>,
    #[serde(default)] no_upload: bool, max_parallel: Option<usize>,
}

async fn run_orchestrator(State(state): State<Arc<AppState>>, Json(req): Json<OrchestratorRequest>)
-> Result<Json<ActionResponse>, StatusCode> {
    let config_dir = state.config_dir.clone();
    let stages = req.stages.map(|s| s.iter().map(|x| x.parse::<Stage>()).collect::<Result<Vec<_>>>())
        .transpose().map_err(|_| StatusCode::BAD_REQUEST)?;
    let limit = req.limit.unwrap_or(50);
    let max_parallel = req.max_parallel.unwrap_or(2);

    let job_id = format!("fleet-{}", chrono::Utc::now().timestamp_millis());
    state.jobs.write().await.insert(job_id.clone(), Job {
        id: job_id.clone(), niche: "fleet".into(), kind: "orchestrator".into(),
        status: "running".into(), message: "started fleet sweep".into(),
    });
    let state2 = Arc::clone(&state);
    let id2 = job_id.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let msg = match run_fleet_sweep(&config_dir, stages, limit, max_parallel, req.no_upload).await {
                Ok(n) => format!("{n} niche(s) processed"),
                Err(e) => format!("error: {e:#}"),
            };
            if let Some(j) = state2.jobs.write().await.get_mut(&id2) {
                j.status = "done".into(); j.message = msg;
            }
        });
    });
    Ok(Json(ActionResponse { job_id, status: "running".into() }))
}

async fn run_fleet_sweep(config_dir: &str, stages: Option<Vec<Stage>>, limit: usize, max_parallel: usize, no_upload: bool)
-> Result<usize> {
    let mut plan = stages.unwrap_or_else(Stage::all);
    if no_upload { plan.retain(|s| *s != Stage::Upload); }
    let configs = discover_configs(config_dir)?;
    if configs.is_empty() { bail!("no configs found"); }
    let sem = Arc::new(tokio::sync::Semaphore::new(max_parallel.max(1)));
    let futures = configs.into_iter().enumerate().map(|(idx, path)| {
        let sem = Arc::clone(&sem);
        let plan = plan.clone();
        async move {
            tokio::time::sleep(tokio::time::Duration::from_millis(750 * idx as u64)).await;
            let _permit = sem.acquire().await.expect("semaphore closed");
            let name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("unknown").to_string();
            let cfg = match load_config(&path) {
                Ok(c) => c,
                Err(e) => { tracing::warn!("[{name}] config load failed: {e:#}"); return Ok(0usize); }
            };
            { let conn = db::open_and_init(&cfg.db_path)?; db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?; }
            let mut passed = 0usize;
            for stage in plan {
                match run_one_stage(stage, &cfg, limit).await {
                    Result::Ok(n) => { tracing::info!("[{name}] {} done :: {n} items", stage.label()); passed += 1; }
                    Result::Err(e) => { tracing::error!("[{name}] {} failed: {e:#}", stage.label()); break; }
                }
            }
            Ok::<usize, anyhow::Error>(passed)
        }
    });
    let results = futures::future::join_all(futures).await;
    Ok(results.into_iter().filter_map(|r| r.ok()).count())
}

async fn run_selector(State(state): State<Arc<AppState>>)
-> Result<Json<ActionResponse>, StatusCode> {
    let config_dir = state.config_dir.clone();
    let job_id = format!("selector-{}", chrono::Utc::now().timestamp_millis());
    state.jobs.write().await.insert(job_id.clone(), Job {
        id: job_id.clone(), niche: "fleet".into(), kind: "selector".into(),
        status: "running".into(), message: "started selector".into(),
    });
    let state2 = Arc::clone(&state);
    let id2 = job_id.clone();
    tokio::task::spawn_blocking(move || {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        rt.block_on(async {
            let msg = match tokio::task::spawn_blocking(move || run_federated_selector(&config_dir)).await {
                Ok(Ok(n)) => format!("wrote {n} plan row(s)"),
                Ok(Err(e)) => format!("error: {e:#}"),
                Err(e) => format!("panic: {e:#}"),
            };
            if let Some(j) = state2.jobs.write().await.get_mut(&id2) {
                j.status = "done".into(); j.message = msg;
            }
        });
    });
    Ok(Json(ActionResponse { job_id, status: "running".into() }))
}

fn run_federated_selector(config_dir: &str) -> Result<usize> {
    let paths = discover_configs(config_dir)?;
    if paths.is_empty() { bail!("no .toml configs found"); }
    let run_date = chrono::Utc::now().format("%Y-%m-%d").to_string();
    let mut scores: std::collections::BTreeMap<selector::NicheKey, f64> = std::collections::BTreeMap::new();
    let mut samples: std::collections::BTreeMap<selector::NicheKey, i64> = std::collections::BTreeMap::new();
    let mut routes: Vec<(selector::NicheKey, rusqlite::Connection)> = Vec::new();
    let mut policy: Option<youtube_pipeline::config::SelectorConfig> = None;

    for path in &paths {
        let path_str = path.to_str().context("non-utf8 config path")?;
        let cfg = AppConfig::load(path_str).with_context(|| format!("loading {path_str}"))?;
        let conn = db::open_and_init(&cfg.db_path).with_context(|| format!("opening db {}", cfg.db_path))?;
        db::set_channel_meta(&conn, &cfg.channel.niche, &cfg.channel.format)?;
        let (s, n) = selector::score_niches(&conn)?;
        scores.extend(s); samples.extend(n);
        let key = (cfg.channel.niche.clone(), cfg.channel.format.clone());
        scores.entry(key.clone()).or_insert(0.0);
        samples.entry(key.clone()).or_insert(0);
        if policy.is_none() { policy = Some(cfg.selector.clone()); }
        routes.push((key, conn));
    }
    let policy = policy.ok_or_else(|| anyhow::anyhow!("no usable niche policy"))?;
    let plan = selector::allocate(&scores, &samples, &policy);
    let mut wrote = 0usize;
    for row in &plan {
        let (niche, format, _quota, _reason) = row;
        if let Some((_, conn)) = routes.iter().find(|(k, _)| &k.0 == niche && &k.1 == format) {
            selector::write_plan(conn, &run_date, std::slice::from_ref(row))?;
            wrote += 1;
        }
    }
    Ok(wrote)
}

async fn list_jobs(State(state): State<Arc<AppState>>)
-> Json<Vec<Job>> {
    let jobs = state.jobs.read().await.values().cloned().collect();
    Json(jobs)
}

const INDEX_HTML: &str = r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>YouTube Pipeline Dashboard</title>
<style>
:root{--bg:#0b0f19;--panel:#111827;--border:#1f2937;--text:#e5e7eb;--muted:#9ca3af;--accent:#3b82f6;--ok:#22c55e;--warn:#f59e0b;--bad:#ef4444;}
*{box-sizing:border-box}
body{margin:0;font-family:system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;background:var(--bg);color:var(--text);line-height:1.45}
header{padding:1rem 1.25rem;border-bottom:1px solid var(--border);display:flex;align-items:center;justify-content:space-between;gap:1rem}
h1{margin:0;font-size:1.25rem}
button{background:var(--accent);color:#fff;border:none;border-radius:6px;padding:.5rem .8rem;cursor:pointer;font-size:.9rem}
button:hover{filter:brightness(1.1)}
button.secondary{background:var(--panel);border:1px solid var(--border);color:var(--text)}
button.warn{background:var(--warn);color:#111}
button.danger{background:var(--bad)}
.container{max-width:1200px;margin:0 auto;padding:1rem}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(320px,1fr));gap:1rem;margin-top:1rem}
.card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:1rem}
.card h2{margin:0 0 .6rem;font-size:1rem;color:var(--muted);text-transform:uppercase;letter-spacing:.03em}
table{width:100%;border-collapse:collapse;font-size:.9rem}
th,td{padding:.45rem .35rem;text-align:left;border-bottom:1px solid var(--border)}
th{color:var(--muted);font-weight:600}
tr:hover{background:#1f2937}
.num{text-align:right;font-variant-numeric:tabular-nums}
.badge{font-size:.72rem;padding:.15rem .4rem;border-radius:4px;background:var(--border);color:var(--muted)}
.badge.ok{background:#14532d;color:#86efac}.badge.warn{background:#713f12;color:#fde047}.badge.bad{background:#7f1d1d;color:#fca5a5}
.actions{display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.6rem}
.section{margin-top:1.5rem}
.section h2{font-size:1.05rem;color:var(--muted);margin:0 0 .6rem}
pre{background:#0b0f19;border:1px solid var(--border);padding:.6rem;border-radius:6px;overflow:auto;font-size:.85rem}
input{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:.45rem .55rem;border-radius:6px;font-size:.9rem;min-width:220px}
.tabs{display:flex;gap:.25rem;border-bottom:1px solid var(--border);margin-bottom:.6rem}
.tab{padding:.4rem .8rem;cursor:pointer;border-radius:6px 6px 0 0;color:var(--muted)}
.tab.active{background:var(--panel);color:var(--text);border:1px solid var(--border);border-bottom:none}
#toast{position:fixed;bottom:1rem;right:1rem;background:var(--panel);border:1px solid var(--border);padding:.6rem .9rem;border-radius:8px;opacity:0;transition:opacity .3s;pointer-events:none}
#toast.show{opacity:1}
</style>
</head>
<body>
<header>
<h1>YouTube Pipeline Dashboard</h1>
<div style="display:flex;gap:.5rem;align-items:center">
  <span class="badge" id="connBadge">loading</span>
  <button onclick="loadAll()">Refresh</button>
</div>
</header>
<div class="container">

<div class="card">
  <h2>Fleet Actions</h2>
  <div class="actions">
    <button onclick="runOrchestrator()">Run Orchestrator</button>
    <button onclick="runSelector()">Run Selector</button>
    <button class="secondary" onclick="showJobs()">Show Jobs</button>
  </div>
</div>

<div class="section"><h2>Niches</h2>
  <div id="nichesGrid" class="grid"></div>
</div>

<div class="section" id="jobsSection" style="display:none">
  <h2>Jobs</h2>
  <div id="jobsBox"></div>
</div>

<div class="section" id="detailSection" style="display:none">
  <h2 id="detailTitle">Detail</h2>
  <div class="tabs" id="detailTabs"></div>
  <div id="detailContent"></div>
</div>

</div>
<div id="toast"></div>

<script>
const API = '';
let currentNiche = '';
let pollTimer = null;

function toast(msg) {
  const t = document.getElementById('toast');
  t.textContent = msg; t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 2500);
}

async function get(path) {
  const r = await fetch(API + path);
  if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
  return r.json();
}
async function post(path, body = {}) {
  const r = await fetch(API + path, { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify(body) });
  if (!r.ok) throw new Error(r.status + ' ' + r.statusText);
  return r.json();
}

function fmt(n) { return n == null ? '-' : n.toLocaleString(); }

function statusBadge(status) {
  if (status === 'done') return '<span class="badge ok">done</span>';
  if (status === 'running') return '<span class="badge warn">running</span>';
  if (status === 'failed') return '<span class="badge bad">failed</span>';
  if (status === 'dead') return '<span class="badge bad">dead</span>';
  return '<span class="badge">'+status+'</span>';
}

function renderNiches(data) {
  const grid = document.getElementById('nichesGrid');
  grid.innerHTML = '';
  for (const n of data.niches) {
    const div = document.createElement('div');
    div.className = 'card';
    let stagesRows = n.stages.map(s =>
      '<tr><td>'+s.stage+'</td>'+
      '<td class="num">'+fmt(s.pending)+'</td>'+
      '<td class="num">'+fmt(s.running)+'</td>'+
      '<td class="num">'+fmt(s.done)+'</td>'+
      '<td class="num">'+fmt(s.failed)+'</td>'+
      '<td class="num">'+fmt(s.dead)+'</td></tr>'
    ).join('');
    div.innerHTML =
      '<div style="display:flex;justify-content:space-between;align-items:center">'+
        '<strong>'+escapeHtml(n.name)+'</strong>'+
        '<span class="badge">'+escapeHtml(n.format)+'</span></div>'+
      '<div style="color:var(--muted);font-size:.85rem;margin:.2rem 0">'+escapeHtml(n.niche)+'</div>'+
      '<table><thead><tr><th>stage</th><th class="num">pending</th><th class="num">run</th><th class="num">done</th><th class="num">fail</th><th class="num">dead</th></tr></thead>'+
      '<tbody>'+stagesRows+'</tbody></table>'+
      '<div class="actions">'+
        '<button onclick="runNiche(\''+escapeHtml(n.niche)+'\')">Run</button>'+
        '<button class="secondary" onclick="showNiche(\''+escapeHtml(n.niche)+'\')">Details</button>'+
        '<button class="secondary" onclick="reapNiche(\''+escapeHtml(n.niche)+'\')">Reap</button>'+
        '<button class="secondary" onclick="retryNiche(\''+escapeHtml(n.niche)+'\')">Retry</button>'+
      '</div>';
    grid.appendChild(div);
  }
}

function escapeHtml(t){const d=document.createElement('div');d.textContent=t;return d.innerHTML;}

async function loadAll() {
  try {
    const data = await get('/api/niches');
    renderNiches(data);
    document.getElementById('connBadge').outerHTML = '<span class="badge ok">connected</span>';
  } catch(e) {
    document.getElementById('connBadge').outerHTML = '<span class="badge bad">disconnected</span>';
    console.error(e);
  }
}

async function runNiche(niche) {
  try { const r = await post('/api/niches/'+encodeURIComponent(niche)+'/run', {no_upload:false}); toast('Job '+r.job_id+' started'); }
  catch(e){ toast('Error: '+e.message); }
}
async function reapNiche(niche) {
  try { const r = await post('/api/niches/'+encodeURIComponent(niche)+'/reap', {}); toast('Reap job '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}
async function retryNiche(niche) {
  const stage = prompt('Stage to retry? (e.g. upload)');
  if(!stage) return;
  try { const r = await post('/api/niches/'+encodeURIComponent(niche)+'/retry', {stage}); toast('Retry job '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}

async function showNiche(niche) {
  currentNiche = niche;
  document.getElementById('detailSection').style.display = 'block';
  document.getElementById('detailTitle').textContent = 'Niche: ' + niche;
  const tabs = document.getElementById('detailTabs');
  tabs.innerHTML =
    '<div class="tab active" onclick="switchTab(this,\'books\')">Books</div>'+
    '<div class="tab" onclick="switchTab(this,\'frames\')">Frames</div>'+
    '<div class="tab" onclick="switchTab(this,\'dead\')">Dead</div>'+
    '<div class="tab" onclick="switchTab(this,\'plan\')">Plan</div>'+
    '<div class="tab" onclick="switchTab(this,\'stats\')">Stats</div>'+
    '<div class="tab" onclick="switchTab(this,\'ingest\')">Ingest</div>'+
    '<div class="tab" onclick="switchTab(this,\'analytics\')">Analytics</div>';
  await switchTab(tabs.children[0], 'books');
}

async function switchTab(el, key) {
  for(const c of el.parentElement.children) c.classList.remove('active');
  el.classList.add('active');
  const box = document.getElementById('detailContent');
  box.innerHTML = '<div class="card">Loading...</div>';
  const n = encodeURIComponent(currentNiche);
  try {
    if (key === 'books') {
      const d = await get('/api/niches/'+n+'/books');
      box.innerHTML = '<div class="card"><table><thead><tr><th>id</th><th>gutenberg</th><th>title</th><th>author</th><th>lang</th><th>year</th></tr></thead><tbody>'+
        d.books.map(b=>'<tr><td>'+b.id+'</td><td>'+b.gutenberg_id+'</td><td>'+escapeHtml(b.title)+'</td><td>'+escapeHtml(b.author)+'</td><td>'+b.language+'</td><td>'+(b.issued_year||'-')+'</td></tr>').join('')+
        '</tbody></table></div>';
    } else if (key === 'frames') {
      const d = await get('/api/niches/'+n+'/frames');
      box.innerHTML = '<div class="card"><table><thead><tr><th>book_id</th><th>hook</th><th>audio</th><th>output</th><th>youtube</th></tr></thead><tbody>'+
        d.frames.map(f=>'<tr><td>'+f.book_id+'</td><td>'+(f.hook_text||'-').slice(0,60)+'</td><td>'+(f.audio_path?'yes':'no')+'</td><td>'+(f.output_path?'yes':'no')+'</td><td>'+(f.youtube_id||'-')+'</td></tr>').join('')+
        '</tbody></table></div>';
    } else if (key === 'dead') {
      const d = await get('/api/niches/'+n+'/dead');
      box.innerHTML = '<div class="card"><table><thead><tr><th>book_id</th><th>stage</th><th>attempts</th><th>last_error</th></tr></thead><tbody>'+
        d.rows.map(r=>'<tr><td>'+r.book_id+'</td><td>'+r.stage+'</td><td>'+r.attempts+'</td><td>'+escapeHtml(r.last_error)+'</td></tr>').join('')+
        '</tbody></table></div>';
    } else if (key === 'plan') {
      const d = await get('/api/niches/'+n+'/plan');
      box.innerHTML = '<div class="card"><table><thead><tr><th>date</th><th>niche</th><th>format</th><th>quota</th><th>reason</th></tr></thead><tbody>'+
        d.plans.map(p=>'<tr><td>'+p.run_date+'</td><td>'+p.niche+'</td><td>'+p.format+'</td><td>'+p.quota+'</td><td>'+escapeHtml(p.reason)+'</td></tr>').join('')+
        '</tbody></table></div>';
    } else if (key === 'stats') {
      const d = await get('/api/niches/'+n+'/stats');
      box.innerHTML = '<div class="card"><table><thead><tr><th>video</th><th>date</th><th>views</th><th>revenue</th></tr></thead><tbody>'+
        d.stats.map(s=>'<tr><td>'+escapeHtml(s.video_id)+'</td><td>'+s.snapshot_date+'</td><td class="num">'+fmt(s.views)+'</td><td class="num">$'+s.est_revenue_usd.toFixed(2)+'</td></tr>').join('')+
        '</tbody></table></div>';
    } else if (key === 'ingest') {
      box.innerHTML = '<div class="card"><h3>Ingest books</h3><div class="actions" style="flex-direction:column;align-items:flex-start">'+
        '<label>IDs (comma-separated):<br><input id="ingestIds" placeholder="84,1342"></label>'+
        '<label>Titles (semicolon-separated):<br><input id="ingestTitles" placeholder="Frankenstein;Dracula"></label>'+
        '<button onclick="doIngest()">Ingest</button></div></div>';
    } else if (key === 'analytics') {
      box.innerHTML = '<div class="card"><p>Pull a fresh analytics snapshot for this niche.</p><button onclick="doAnalytics()">Run Analytics</button></div>';
    }
  } catch(e) {
    box.innerHTML = '<div class="card" style="color:var(--bad)">Error: '+escapeHtml(e.message)+'</div>';
  }
}

async function doIngest() {
  const ids = document.getElementById('ingestIds').value.split(',').map(x=>parseInt(x.trim())).filter(x=>!isNaN(x));
  const titles = document.getElementById('ingestTitles').value.split(';').map(x=>x.trim()).filter(Boolean);
  const body = {}; if(ids.length) body.ids=ids; if(titles.length) body.titles=titles;
  try { const r = await post('/api/niches/'+encodeURIComponent(currentNiche)+'/ingest', body); toast('Ingest job '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}
async function doAnalytics() {
  try { const r = await post('/api/niches/'+encodeURIComponent(currentNiche)+'/analytics', {}); toast('Analytics job '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}

async function runOrchestrator() {
  try { const r = await post('/api/orchestrator/run', {no_upload:false}); toast('Orchestrator '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}
async function runSelector() {
  try { const r = await post('/api/selector/run', {}); toast('Selector '+r.job_id); }
  catch(e){ toast('Error: '+e.message); }
}

async function showJobs() {
  const sec = document.getElementById('jobsSection');
  sec.style.display = 'block';
  try {
    const jobs = await get('/api/jobs');
    const box = document.getElementById('jobsBox');
    if (!jobs.length) { box.innerHTML = '<div class="card">No jobs yet.</div>'; return; }
    box.innerHTML = '<div class="card"><table><thead><tr><th>id</th><th>niche</th><th>kind</th><th>status</th><th>message</th></tr></thead><tbody>'+
      jobs.map(j=>'<tr><td>'+escapeHtml(j.id)+'</td><td>'+escapeHtml(j.niche)+'</td><td>'+escapeHtml(j.kind)+'</td><td>'+statusBadge(j.status)+'</td><td>'+escapeHtml(j.message)+'</td></tr>').join('')+
      '</tbody></table></div>';
  } catch(e) { document.getElementById('jobsBox').innerHTML = '<div class="card" style="color:var(--bad)">Error</div>'; }
}

loadAll();
if (pollTimer) clearInterval(pollTimer);
pollTimer = setInterval(loadAll, 5000);
</script>
</body>
</html>"##;
