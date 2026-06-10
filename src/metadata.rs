use anyhow::{anyhow, Context, Result};
use rusqlite::{params, Connection};
use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Clone, Deserialize)]
pub struct MetadataConfig {
    pub api_base: String,
    pub api_key: String,
    pub model: String,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_title_chars")]
    pub max_title_chars: usize,
    #[serde(default = "default_desc_chars")]
    pub max_desc_chars: usize,
    #[serde(default = "default_max_tags")]
    pub max_tags: usize,
    #[serde(default)]
    pub channel_blurb: String,
}

fn default_timeout() -> u64 {
    30
}
fn default_concurrency() -> usize {
    3
}
fn default_title_chars() -> usize {
    100
}
fn default_desc_chars() -> usize {
    5000
}
fn default_max_tags() -> usize {
    15
}

impl MetadataConfig {
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("reading metadata config {path}"))?;
        let resolved = resolve_env_vars(&raw)?;
        toml::from_str(&resolved).context("parsing metadata config")
    }
}

fn resolve_env_vars(input: &str) -> Result<String> {
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| anyhow!("unterminated ${{...}} in config"))?;
        let var = &after[..end];
        let val = std::env::var(var).with_context(|| format!("env var {var} not set"))?;
        out.push_str(&val);
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct BookMeta {
    pub book_id: i64,
    pub title: String,
    pub author: String,
    pub hook_text: String,
    pub subjects: String,
}

#[derive(Debug, Clone)]
pub struct VideoMetadata {
    pub title: String,
    pub description: String,
    pub tags: Vec<String>,
}

#[derive(serde::Serialize)]
struct ChatRequest<'a> {
    model: &'a str,
    messages: Vec<ChatMessage<'a>>,
    temperature: f32,
    response_format: ResponseFormat,
}

#[derive(serde::Serialize)]
struct ChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(serde::Serialize)]
struct ResponseFormat {
    #[serde(rename = "type")]
    kind: &'static str,
}

#[derive(Deserialize)]
struct ChatResponse {
    choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct ChatChoice {
    message: ChatContent,
}

#[derive(Deserialize)]
struct ChatContent {
    content: String,
}

#[derive(Deserialize)]
struct LlmMeta {
    title: String,
    description: String,
    tags: Vec<String>,
}

pub fn build_metadata_deterministic(cfg: &MetadataConfig, book: &BookMeta) -> VideoMetadata {
    let raw_title = if !book.hook_text.trim().is_empty() {
        book.hook_text.clone()
    } else if !book.author.trim().is_empty() {
        format!("{} — {}", book.title, book.author)
    } else {
        book.title.clone()
    };
    let title = truncate_clean(&raw_title, cfg.max_title_chars);

    let mut desc = String::new();
    desc.push_str(&book.hook_text);
    if !desc.is_empty() {
        desc.push_str("\n\n");
    }
    if !book.author.trim().is_empty() {
        desc.push_str(&format!("From \"{}\" by {}.\n", book.title, book.author));
    } else {
        desc.push_str(&format!("From \"{}\".\n", book.title));
    }
    desc.push_str("A public-domain work, read and reflected on.\n\n");
    desc.push_str(&cfg.channel_blurb);
    let description = truncate_clean(&desc, cfg.max_desc_chars);

    let mut tags: Vec<String> = Vec::new();
    if !book.author.trim().is_empty() {
        tags.push(book.author.clone());
    }
    for w in book.title.split_whitespace() {
        let w = w.trim_matches(|c: char| !c.is_alphanumeric());
        if w.len() > 3 {
            tags.push(w.to_lowercase());
        }
    }
    for s in book.subjects.split(',') {
        let s = s.trim();
        if !s.is_empty() {
            tags.push(s.to_lowercase());
        }
    }
    tags.extend([
        "classic literature".to_string(),
        "public domain".to_string(),
        "audiobook".to_string(),
        "forgotten classics".to_string(),
    ]);
    dedup_truncate_tags(&mut tags, cfg.max_tags);

    VideoMetadata {
        title,
        description,
        tags,
    }
}

pub async fn build_metadata_llm(cfg: &MetadataConfig, book: &BookMeta) -> VideoMetadata {
    match try_llm(cfg, book).await {
        Ok(meta) => meta,
        Err(e) => {
            eprintln!(
                "llm metadata failed for {} ({e:#}); using fallback",
                book.book_id
            );
            build_metadata_deterministic(cfg, book)
        }
    }
}

async fn try_llm(cfg: &MetadataConfig, book: &BookMeta) -> Result<VideoMetadata> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(cfg.timeout_secs))
        .build()?;

    let system = "You write YouTube metadata for a faceless channel about \
public-domain classic books. Output strict JSON with keys: title (string, \
punchy, curiosity-driven, no clickbait lies), description (string, 2-4 short \
paragraphs, no hashtags inside body), tags (array of 8-15 lowercase search \
keywords). Do not invent facts about the book.";

    let user = format!(
        "Book title: {}\nAuthor: {}\nGutenberg subjects: {}\nApproved hook line: {}\n\n\
Write the metadata. The title may reuse or sharpen the hook. Keep title under {} characters.",
        book.title,
        if book.author.is_empty() {
            "(unknown)"
        } else {
            &book.author
        },
        if book.subjects.is_empty() {
            "(none)"
        } else {
            &book.subjects
        },
        book.hook_text,
        cfg.max_title_chars,
    );

    let req = ChatRequest {
        model: &cfg.model,
        messages: vec![
            ChatMessage {
                role: "system",
                content: system,
            },
            ChatMessage {
                role: "user",
                content: &user,
            },
        ],
        temperature: 0.8,
        response_format: ResponseFormat {
            kind: "json_object",
        },
    };

    let resp = client
        .post(format!(
            "{}/chat/completions",
            cfg.api_base.trim_end_matches('/')
        ))
        .bearer_auth(&cfg.api_key)
        .json(&req)
        .send()
        .await
        .context("sending chat request")?;

    let status = resp.status();
    let body = resp.text().await.context("reading response body")?;
    if !status.is_success() {
        return Err(anyhow!("api returned {}: {}", status, body.trim()));
    }

    let parsed: ChatResponse = serde_json::from_str(&body).context("parsing chat envelope")?;
    let content = parsed
        .choices
        .first()
        .ok_or_else(|| anyhow!("no choices in response"))?
        .message
        .content
        .as_str();

    let llm: LlmMeta = serde_json::from_str(content).context("parsing model JSON content")?;

    let title = truncate_clean(&llm.title, cfg.max_title_chars);
    let mut description = llm.description;
    if !cfg.channel_blurb.is_empty() {
        description.push_str("\n\n");
        description.push_str(&cfg.channel_blurb);
    }
    let description = truncate_clean(&description, cfg.max_desc_chars);

    let mut tags = llm.tags;
    dedup_truncate_tags(&mut tags, cfg.max_tags);

    if title.trim().is_empty() || description.trim().is_empty() {
        return Err(anyhow!("model returned empty title or description"));
    }

    Ok(VideoMetadata {
        title,
        description,
        tags,
    })
}

fn truncate_clean(s: &str, max_chars: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let mut out = String::new();
    let mut count = 0;
    for word in s.split_whitespace() {
        let wlen = word.chars().count();
        if count + wlen + 1 > max_chars {
            break;
        }
        if !out.is_empty() {
            out.push(' ');
            count += 1;
        }
        out.push_str(word);
        count += wlen;
    }
    out.trim_end_matches([',', '-', '—']).trim().to_string()
}

fn dedup_truncate_tags(tags: &mut Vec<String>, max_tags: usize) {
    let mut seen = std::collections::HashSet::new();
    let mut kept: Vec<String> = Vec::new();
    let mut total_chars = 0usize;

    for t in tags.iter() {
        let t = t.trim().to_lowercase();
        if t.is_empty() || !seen.insert(t.clone()) {
            continue;
        }
        if total_chars + t.len() + 1 > 480 {
            break;
        }
        total_chars += t.len() + 1;
        kept.push(t);
        if kept.len() >= max_tags {
            break;
        }
    }
    *tags = kept;
}

/// Single-item entry point used by the orchestrator's run_one dispatch.
pub async fn run_one(conn: &Connection, cfg: &MetadataConfig, book_id: i64) -> Result<()> {
    let book = fetch_one(conn, book_id)?;
    let meta = build_metadata_llm(cfg, &book).await;
    store_metadata(conn, book_id, &meta)?;
    Ok(())
}

fn fetch_one(db: &Connection, book_id: i64) -> Result<BookMeta> {
    db.execute_batch("ALTER TABLE script_frames ADD COLUMN yt_title TEXT;")
        .ok();
    db.execute_batch("ALTER TABLE script_frames ADD COLUMN yt_description TEXT;")
        .ok();
    db.execute_batch("ALTER TABLE script_frames ADD COLUMN yt_tags TEXT;")
        .ok();

    db.query_row(
        "SELECT sf.book_id, b.title, b.author, sf.hook_text, COALESCE(b.subjects, '') \
         FROM script_frames sf JOIN books b ON b.id = sf.book_id WHERE sf.book_id = ?1",
        params![book_id],
        |r| {
            Ok(BookMeta {
                book_id: r.get(0)?,
                title: r.get(1)?,
                author: r.get::<_, Option<String>>(2)?.unwrap_or_default(),
                hook_text: r.get(3)?,
                subjects: r.get(4)?,
            })
        },
    )
    .with_context(|| format!("fetching book {book_id} for metadata"))
}

fn store_metadata(db: &Connection, book_id: i64, meta: &VideoMetadata) -> Result<()> {
    let tags_blob = meta.tags.join("\n");
    db.execute(
        "UPDATE script_frames SET yt_title = ?1, yt_description = ?2, yt_tags = ?3 WHERE book_id = ?4",
        params![meta.title, meta.description, tags_blob, book_id],
    )?;
    Ok(())
}
