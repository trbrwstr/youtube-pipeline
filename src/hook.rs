// src/hook.rs
//
// Hook-frame generation pipeline (consolidated):
//   1. best_excerpt        — picks the grabbiest opening sentence, not just the first
//   2. fetch_unscripted    — quality gate: skips stubs and junk-title books
//   3. build_hook_llm      — retries with a STRICT prompt when a hook runs long
//   4. open_db             — WAL mode for better read/write concurrency
//   5. CostGuard           — tracks approximate LLM token spend, halts at a ceiling
//
// Cargo.toml deps:
//   reqwest   = { version = "0.12", features = ["json"] }
//   tokio     = { version = "1", features = ["full"] }
//   rusqlite  = { version = "0.31", features = ["bundled"] }
//   serde     = { version = "1", features = ["derive"] }
//   toml      = "0.8"
//   anyhow    = "1"
//   futures   = "0.3"

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

// ============================================================
//  Domain types
// ============================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Book {
    pub id: i64,
    pub title: String,
    pub author: String,
    pub year: Option<i32>,
    pub body: String, // cleaned text, boilerplate stripped
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptFrame {
    pub hook: String,       // on-screen text
    pub voice_line: String, // TTS reads this
    pub duration_secs: f32, // estimated read time
}

// ============================================================
//  Config
// ============================================================

#[derive(Debug, Clone, Deserialize)]
pub struct HookConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
    pub wpm: f32,
    pub max_hook_secs: f32,
    pub system_prompt: String,
    pub timeout_secs: u64,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    // --- quality gate (fix #2) ---
    #[serde(default = "default_min_body_chars")]
    pub min_body_chars: usize,
    // --- cost guard (fix #5) ---
    #[serde(default = "default_max_tokens_budget")]
    pub max_tokens_budget: u64,
    #[serde(default = "default_usd_per_1k_tokens")]
    pub usd_per_1k_tokens: f64,
}

fn default_concurrency() -> usize {
    6
}
fn default_min_body_chars() -> usize {
    2_000
}
fn default_max_tokens_budget() -> u64 {
    200_000
}
fn default_usd_per_1k_tokens() -> f64 {
    0.0006
}

#[derive(Debug, Clone, Deserialize)]
struct ConfigFile {
    hook: HookConfig,
}

impl HookConfig {
    /// Load from a TOML file and resolve any `${ENV_VAR}` values.
    pub fn load(path: &str) -> Result<Self> {
        let raw =
            std::fs::read_to_string(path).with_context(|| format!("reading config {}", path))?;
        let mut cfg: ConfigFile = toml::from_str(&raw).context("parsing config TOML")?;
        cfg.hook.api_key = resolve_env(&cfg.hook.api_key)?;
        cfg.hook.api_base = resolve_env(&cfg.hook.api_base)?;
        Ok(cfg.hook)
    }
}

/// Expand a `${VAR}` reference at load time; pass through literals untouched.
fn resolve_env(value: &str) -> Result<String> {
    let trimmed = value.trim();
    if let Some(inner) = trimmed.strip_prefix("${").and_then(|s| s.strip_suffix('}')) {
        std::env::var(inner).with_context(|| format!("env var {} not set", inner))
    } else {
        Ok(value.to_string())
    }
}

// ============================================================
//  Cost guard (fix #5)
// ============================================================

/// Process-wide approximate token spend tracker. Each LLM call estimates its
/// tokens (chars/4 heuristic — close enough for a budget tripwire) and adds to
/// the running total. Once the ceiling is hit, `try_charge` denies further
/// spend and callers fall back to the free deterministic path.
#[derive(Debug)]
pub struct CostGuard {
    spent: AtomicU64,
    budget: u64,
    usd_per_1k: f64,
}

impl CostGuard {
    pub fn new(budget: u64, usd_per_1k: f64) -> Self {
        Self {
            spent: AtomicU64::new(0),
            budget,
            usd_per_1k,
        }
    }

    /// Reserve `estimate` tokens. Returns false if it would breach the budget,
    /// in which case the caller should use the deterministic builder instead.
    pub fn try_charge(&self, estimate: u64) -> bool {
        let prev = self.spent.fetch_add(estimate, Ordering::SeqCst);
        if prev + estimate > self.budget {
            // roll back the over-charge so the reported total stays honest
            self.spent.fetch_sub(estimate, Ordering::SeqCst);
            false
        } else {
            true
        }
    }

    pub fn spent_tokens(&self) -> u64 {
        self.spent.load(Ordering::SeqCst)
    }

    pub fn spent_usd(&self) -> f64 {
        (self.spent_tokens() as f64 / 1_000.0) * self.usd_per_1k
    }
}

/// Rough token estimate from char count (~4 chars/token for English prose).
pub(crate) fn estimate_tokens(text: &str) -> u64 {
    ((text.chars().count() as f64) / 4.0).ceil() as u64
}

// ============================================================
//  Text helpers
// ============================================================

/// Words-per-minute -> seconds estimate.
fn estimate_secs(text: &str, wpm: f32) -> f32 {
    let words = text.split_whitespace().count() as f32;
    ((words / wpm) * 60.0).max(1.0)
}

/// Split a chunk of prose into rough sentences on terminal punctuation.
fn split_sentences(body: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    for ch in body.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?') {
            let trimmed = current.trim();
            if trimmed.len() > 20 {
                sentences.push(trimmed.to_string());
            }
            current.clear();
        }
    }
    let tail = current.trim();
    if tail.len() > 20 {
        sentences.push(tail.to_string());
    }
    sentences
}

/// FIX #1 — best_excerpt: instead of grabbing the literal first sentence (which
/// is often a chapter heading, a date, or a dull throat-clear), score the first
/// handful of sentences and pick the grabbiest one inside the char budget.
///
/// Scoring rewards: question/exclamation energy, concrete length in a sweet
/// spot, first-person or dialogue markers; penalizes: ALL-CAPS headings,
/// roman-numeral chapter labels, and overly short fragments.
fn best_excerpt(body: &str, max_chars: usize) -> String {
    let candidates: Vec<String> = split_sentences(body.trim_start())
        .into_iter()
        .take(8) // only mine the opening — that's what a viewer hears first
        .filter(|s| s.chars().count() <= max_chars)
        .collect();

    if candidates.is_empty() {
        // hard fallback: clip the opening to the budget on a char boundary
        return body
            .trim_start()
            .chars()
            .take(max_chars)
            .collect::<String>()
            .trim()
            .to_string();
    }

    let best = candidates
        .iter()
        .max_by(|a, b| {
            score_excerpt(a)
                .partial_cmp(&score_excerpt(b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .cloned()
        .unwrap_or_default();

    best.trim().to_string()
}

fn score_excerpt(s: &str) -> f32 {
    let len = s.chars().count() as f32;
    let mut score = 0.0_f32;

    // Sweet spot: 60–120 chars reads well in ~5s. Penalize drift from it.
    score -= (len - 90.0).abs() / 90.0;

    // Energy markers.
    if s.contains('?') {
        score += 1.2;
    }
    if s.contains('!') {
        score += 0.8;
    }
    if s.contains('"') || s.contains('\u{201C}') {
        score += 0.6;
    } // dialogue

    // First-person / intimacy pulls the viewer in.
    let lower = s.to_lowercase();
    if lower.starts_with("i ") || lower.contains(" i ") {
        score += 0.4;
    }

    // Penalize chapter-heading junk.
    let alpha: String = s.chars().filter(|c| c.is_alphabetic()).collect();
    if !alpha.is_empty() {
        let upper = alpha.chars().filter(|c| c.is_uppercase()).count() as f32;
        if upper / alpha.chars().count() as f32 > 0.6 {
            score -= 2.0; // mostly-caps => heading/label
        }
    }
    if lower.starts_with("chapter") || lower.starts_with("part ") {
        score -= 2.0;
    }

    score
}

// ============================================================
//  Deterministic baseline (free, never fails)
// ============================================================

pub fn build_hook(book: &Book, wpm: f32) -> ScriptFrame {
    let author_clause = match book.year {
        Some(y) => format!("{}, written in {}", book.author, y),
        None => book.author.clone(),
    };
    let teaser = best_excerpt(&book.body, 140);
    let voice_line = format!(
        "Over a century ago, {} put these words to paper. Listen. \"{}\"",
        author_clause, teaser
    );
    let hook = format!("\"{}\"\n— {}", teaser, book.author);
    let duration = estimate_secs(&voice_line, wpm);
    ScriptFrame {
        hook,
        voice_line,
        duration_secs: duration,
    }
}

// ============================================================
//  LLM-backed hook with retry + deterministic fallback
// ============================================================

#[derive(Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    temperature: f32,
    max_tokens: u32,
}

#[derive(Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: String,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Deserialize)]
struct Choice {
    message: ResponseMessage,
}

#[derive(Deserialize)]
struct ResponseMessage {
    content: String,
}

/// LLM hook with cost guard, too-long retry, and a hard deterministic fallback —
/// never returns Err to the caller.
pub async fn build_hook_llm(
    book: &Book,
    cfg: &HookConfig,
    client: &reqwest::Client,
    cost: &CostGuard,
) -> ScriptFrame {
    // FIX #5 — pre-flight cost check. Rough estimate of the prompt + a generous
    // completion allowance; if it would blow the budget, don't even dial out.
    let excerpt = best_excerpt(&book.body, 240);
    let est = estimate_tokens(&cfg.system_prompt)
        + estimate_tokens(&excerpt)
        + estimate_tokens(&book.title)
        + 160; // headroom for the completion
    if !cost.try_charge(est) {
        eprintln!(
            "[hook] budget reached ({} tok); deterministic for book {} ({})",
            cost.spent_tokens(),
            book.id,
            book.title
        );
        return build_hook(book, cfg.wpm);
    }

    match try_llm_hook(book, cfg, client, false).await {
        Ok(frame) => frame,
        // FIX #3 — first attempt ran long; retry once with a STRICT prompt that
        // demands a shorter line before giving up to the deterministic path.
        Err(HookError::TooLong { secs }) => {
            eprintln!(
                "[hook] book {} hook {:.1}s > {:.1}s ceiling; STRICT retry",
                book.id, secs, cfg.max_hook_secs
            );
            cost.try_charge(est / 2); // the retry is a second, smaller call
            match try_llm_hook(book, cfg, client, true).await {
                Ok(frame) => frame,
                Err(e) => {
                    eprintln!(
                        "[hook] STRICT retry failed for book {} ({}); fallback: {}",
                        book.id, book.title, e
                    );
                    build_hook(book, cfg.wpm)
                }
            }
        }
        Err(e) => {
            eprintln!(
                "[hook] LLM failed for book {} ({}); deterministic fallback: {}",
                book.id, book.title, e
            );
            build_hook(book, cfg.wpm)
        }
    }
}

/// Typed errors so the caller can distinguish "too long, worth a retry" from
/// "infra blew up, just fall back."
#[derive(Debug)]
enum HookError {
    TooLong { secs: f32 },
    Other(anyhow::Error),
}

impl std::fmt::Display for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HookError::TooLong { secs } => write!(f, "hook too long: {secs:.1}s"),
            HookError::Other(e) => write!(f, "{e}"),
        }
    }
}

impl From<anyhow::Error> for HookError {
    fn from(e: anyhow::Error) -> Self {
        HookError::Other(e)
    }
}

async fn try_llm_hook(
    book: &Book,
    cfg: &HookConfig,
    client: &reqwest::Client,
    strict: bool,
) -> std::result::Result<ScriptFrame, HookError> {
    let excerpt = best_excerpt(&book.body, 240);

    // FIX #3 — the STRICT variant tightens the word budget and screams brevity.
    let length_clause = if strict {
        format!(
            "HARD LIMIT: the hook MUST be readable aloud in under {:.0} seconds \
             at {:.0} wpm — that's roughly {} words MAX. Be ruthless. One short \
             sentence is fine. Do not exceed it.",
            cfg.max_hook_secs,
            cfg.wpm,
            ((cfg.max_hook_secs / 60.0) * cfg.wpm).floor() as i32
        )
    } else {
        "Keep it tight — one or two short sentences.".to_string()
    };

    let user_prompt = format!(
        "Title: {}\nAuthor: {}\nYear: {}\nOpening excerpt:\n\"{}\"\n\n\
         Write a punchy spoken hook for a faceless YouTube short. End on a \
         cliffhanger. No preamble, no surrounding quotes, just the hook. {}",
        book.title,
        book.author,
        book.year
            .map(|y| y.to_string())
            .unwrap_or_else(|| "unknown".into()),
        excerpt,
        length_clause,
    );

    let req = ChatRequest {
        model: &cfg.model,
        messages: vec![
            ChatMessage {
                role: "system",
                content: cfg.system_prompt.clone(),
            },
            ChatMessage {
                role: "user",
                content: user_prompt,
            },
        ],
        temperature: if strict { 0.5 } else { 0.8 },
        max_tokens: if strict { 80 } else { 120 },
    };

    let resp = client
        .post(format!(
            "{}/chat/completions",
            cfg.api_base.trim_end_matches('/')
        ))
        .bearer_auth(&cfg.api_key)
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .json(&req)
        .send()
        .await
        .context("sending chat request")?
        .error_for_status()
        .context("non-2xx from LLM endpoint")?
        .json::<ChatResponse>()
        .await
        .context("parsing chat response")?;

    let voice_line = resp
        .choices
        .into_iter()
        .next()
        .map(|c| c.message.content.trim().to_string())
        .filter(|s| !s.is_empty())
        .context("empty LLM completion")?;

    let duration = estimate_secs(&voice_line, cfg.wpm);
    if duration > cfg.max_hook_secs {
        return Err(HookError::TooLong { secs: duration });
    }

    let hook = format!("{}\n— {}", voice_line, book.author);
    Ok(ScriptFrame {
        hook,
        voice_line,
        duration_secs: duration,
    })
}

// ============================================================
//  SQLite: open + schema + read/write
// ============================================================

/// FIX #4 — open the DB in WAL mode so readers don't block the writer and the
/// writer doesn't block readers. NORMAL synchronous is the right pairing with
/// WAL for a batch job: durable enough, far fewer fsyncs. busy_timeout keeps a
/// momentarily-locked write from erroring out instantly.
pub fn open_db(path: &str) -> Result<Connection> {
    let conn = Connection::open(path).with_context(|| format!("opening sqlite {path}"))?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .context("set WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .context("set synchronous")?;
    conn.pragma_update(None, "busy_timeout", 5_000)
        .context("set busy_timeout")?;
    Ok(conn)
}

/// FIX #2 — quality gate. Pull books that don't yet have a script frame AND
/// clear the bar: a real body (not a stub/index), and a title that isn't an
/// obvious non-readable artifact (indexes, tables of contents, bibliographies).
/// Keeps junk out of the LLM spend and off the channel.
pub fn fetch_unscripted(
    conn: &Connection,
    limit: usize,
    min_body_chars: usize,
) -> Result<Vec<Book>> {
    let mut stmt = conn.prepare(
        "SELECT b.id, b.title, b.author, b.issued_year, b.body
         FROM books b
         LEFT JOIN script_frames s ON s.book_id = b.id
         WHERE (s.book_id IS NULL OR s.hook_text IS NULL OR s.hook_text = '')
           AND b.body IS NOT NULL
           AND length(b.body) >= ?1
           AND b.title IS NOT NULL
           AND length(trim(b.title)) > 0
           AND lower(b.title) NOT LIKE '%index of%'
           AND lower(b.title) NOT LIKE '%table of contents%'
           AND lower(b.title) NOT LIKE '%bibliography%'
           AND lower(b.title) NOT LIKE '%catalogue%'
           AND lower(b.title) NOT LIKE '%dictionary%'
           AND lower(b.title) NOT LIKE '%encyclopaedia%'
         ORDER BY length(b.body) DESC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![min_body_chars as i64, limit as i64], |r| {
            Ok(Book {
                id: r.get(0)?,
                title: r.get(1)?,
                author: r.get(2)?,
                year: r.get(3)?,
                body: r.get(4)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<Book>>>()
        .context("collecting unscripted books")?;
    Ok(rows)
}

/// Fetch a single book by internal id for the per-item `run_one` path.
fn fetch_book(conn: &Connection, book_id: i64) -> Result<Book> {
    conn.query_row(
        "SELECT id, title, author, issued_year, body FROM books WHERE id = ?1",
        params![book_id],
        |r| {
            Ok(Book {
                id: r.get(0)?,
                title: r.get(1)?,
                author: r.get(2)?,
                year: r.get(3)?,
                body: r.get::<_, Option<String>>(4)?.unwrap_or_default(),
            })
        },
    )
    .with_context(|| format!("fetching book {book_id} for hook"))
}

/// Persist the spoken hook line onto this book's script_frames row, creating it
/// if hook is the first stage to touch the book.
fn upsert_hook(conn: &Connection, book_id: i64, hook_text: &str) -> Result<()> {
    conn.execute(
        "INSERT INTO script_frames (book_id, hook_text) VALUES (?1, ?2)
         ON CONFLICT(book_id) DO UPDATE SET hook_text = excluded.hook_text",
        params![book_id, hook_text],
    )
    .with_context(|| format!("storing hook for book {book_id}"))?;
    Ok(())
}

/// Stage entry point: generate (LLM with deterministic fallback) and store the
/// hook for one book. Idempotent — a book that already has a non-empty
/// `hook_text` is a no-op so re-runs don't re-spend on the LLM. The HTTP client
/// is shared across the batch by the runner (connection-pool reuse).
pub async fn run_one(
    conn: &Connection,
    cfg: &HookConfig,
    client: &reqwest::Client,
    cost: &CostGuard,
    book_id: i64,
) -> Result<()> {
    let existing: Option<String> = conn
        .query_row(
            "SELECT hook_text FROM script_frames WHERE book_id = ?1",
            params![book_id],
            |r| r.get(0),
        )
        .ok()
        .flatten();
    if existing.map(|s| !s.trim().is_empty()).unwrap_or(false) {
        return Ok(());
    }

    let book = fetch_book(conn, book_id)?;
    if book.body.trim().is_empty() {
        anyhow::bail!("book {book_id} has no body to hook");
    }

    // build_hook_llm never errors — it falls back to the deterministic builder.
    // `cost` is the batch-wide budget shared by every worker in this run.
    let frame = build_hook_llm(&book, cfg, client, cost).await;
    upsert_hook(conn, book_id, &frame.voice_line)?;
    Ok(())
}
