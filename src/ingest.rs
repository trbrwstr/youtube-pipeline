// src/ingest.rs
//
// Stage 1: ingest. Fetch + parse the Project Gutenberg catalog, filter to
// candidate books (English, Text, non-empty title, plausibly old), strip the
// PG license boilerplate, and store rows to SQLite for downstream stages.
//
// Resumable: the catalog fetch uses ETag caching (skips the ~80MB download if
// unchanged), and per-book text fetches skip anything already stored.
//
// Cargo.toml deps (workspace set):
// reqwest    = { version = "0.12", features = ["stream"] }
// tokio      = { version = "1", features = ["full"] }
// rusqlite   = { version = "0.31", features = ["bundled"] }
// anyhow     = "1"
// csv        = "1"
// flate2     = "1"
// serde      = { version = "1", features = ["derive"] }

use anyhow::{anyhow, Context, Result};
use flate2::read::GzDecoder;
use serde::Deserialize;
use std::io::Read;

use crate::throttle::Throttle;

const CATALOG_URL: &str = "https://www.gutenberg.org/cache/epub/feeds/pg_catalog.csv.gz";
const PG_HOST: &str = "gutenberg.org";

/// Per-niche ingest config, deserialized as `[ingest]` in the TOML and exposed
/// on AppConfig as `cfg.ingest`.
#[derive(Debug, Clone, Deserialize)]
pub struct IngestConfig {
    /// Language code to keep, e.g. "en".
    #[serde(default = "default_language")]
    pub language: String,

    /// Only keep rows whose `Issued` year is <= this. NOTE: Issued is the PG
    /// *upload* date, not original publication — this is a coarse net, not a
    /// real copyright filter. Set to a year that lets old works through and
    /// accept that some modern uploads of old books slip in.
    #[serde(default = "default_max_issued_year")]
    pub max_issued_year: i32,

    /// Where to cache the catalog gzip + the stored ETag.
    #[serde(default = "default_cache_dir")]
    pub cache_dir: String,

    /// Skip the per-book full-text fetch during ingest (catalog rows only).
    /// Downstream `hook`/`tts` can lazy-fetch text instead if you prefer.
    #[serde(default)]
    pub fetch_text: bool,
}

fn default_language() -> String {
    "en".to_string()
}
fn default_max_issued_year() -> i32 {
    1928
}
fn default_cache_dir() -> String {
    "cache".to_string()
}

/// A catalog row, post-filter and (optionally) post-text-fetch. Mirrors the
/// `books` table columns the rest of the pipeline reads.
#[derive(Debug, Clone)]
pub struct Book {
    pub gutenberg_id: i64,
    pub title: String,
    pub author: String,
    pub language: String,
    pub issued_year: Option<i32>,
    pub text_url: Option<String>,
    pub body: Option<String>,
}

/// Raw shape of one `pg_catalog.csv` record. PG's column headers are
/// Title-Cased with spaces; serde rename maps them.
#[derive(Debug, Deserialize)]
struct CatalogRow {
    #[serde(rename = "Text#")]
    id: String,
    #[serde(rename = "Type")]
    kind: String,
    #[serde(rename = "Issued")]
    issued: String,
    #[serde(rename = "Title")]
    title: String,
    #[serde(rename = "Language")]
    language: String,
    #[serde(rename = "Authors")]
    authors: String,
}

// ============================================================
// Public entry point — called by pipeline.rs run_stage(Ingest).
// ============================================================

/// Fetch catalog, filter, (optionally) fetch text, store. Returns the count of
/// books newly inserted this run (already-present rows don't count).
pub async fn run_batch(db_path: &str, cfg: &IngestConfig, limit: usize) -> Result<usize> {
    let throttle = Throttle::from_default();
    let client = reqwest::Client::builder()
        .user_agent("yt-pipeline-ingest/0.1 (+contact)")
        .build()?;

    // 1. Pull the catalog bytes (ETag cache short-circuits the download).
    let csv_bytes = fetch_catalog(&client, cfg)
        .await
        .context("fetching pg_catalog.csv.gz")?;

    // 2. Parse + filter into candidate books.
    let candidates = parse_and_filter(&csv_bytes, cfg).context("parsing catalog CSV")?;
    println!("ingest: {} candidate(s) after filter", candidates.len());

    // 3. Store rows, skipping ones already in the DB, capped at `limit`.
    let mut conn =
        rusqlite::Connection::open(db_path).with_context(|| format!("opening db {db_path}"))?;

    let mut inserted = 0usize;
    for mut book in candidates {
        if inserted >= limit {
            break;
        }
        if book_exists(&conn, book.gutenberg_id)? {
            continue;
        }

        // 4. Optionally hydrate full text now. On fetch failure we still store
        //    the catalog row (body=None) so a later text-only pass can retry.
        if cfg.fetch_text {
            match fetch_book_text(&client, &throttle, book.gutenberg_id).await {
                Ok((url, body)) => {
                    book.text_url = Some(url);
                    book.body = Some(strip_pg_boilerplate(&body));
                }
                Err(e) => {
                    eprintln!(
                        "ingest: text fetch failed for #{} ({}): {e:#}",
                        book.gutenberg_id, book.title
                    );
                }
            }
        }

        insert_book(&mut conn, &book)
            .with_context(|| format!("inserting book #{}", book.gutenberg_id))?;
        inserted += 1;
    }

    Ok(inserted)
}

// ============================================================
// Catalog fetch with ETag caching
// ============================================================

/// Fetch the gzipped catalog, decompress, return raw CSV bytes. If a cached
/// copy + stored ETag exist and the server replies 304, reuse the cache and
/// skip the ~80MB transfer entirely.
async fn fetch_catalog(client: &reqwest::Client, cfg: &IngestConfig) -> Result<Vec<u8>> {
    std::fs::create_dir_all(&cfg.cache_dir)
        .with_context(|| format!("creating cache dir {}", cfg.cache_dir))?;

    let gz_path = format!("{}/pg_catalog.csv.gz", cfg.cache_dir);
    let etag_path = format!("{}/pg_catalog.etag", cfg.cache_dir);

    let prev_etag = std::fs::read_to_string(&etag_path).ok();

    let mut req = client.get(CATALOG_URL);
    if let Some(tag) = &prev_etag {
        // Conditional GET: server returns 304 + empty body if unchanged.
        req = req.header(reqwest::header::IF_NONE_MATCH, tag.trim());
    }

    let resp = req.send().await.context("GET catalog")?;
    let status = resp.status();

    let gz_bytes: Vec<u8> = if status == reqwest::StatusCode::NOT_MODIFIED {
        // Cache hit. Read the stored gzip off disk.
        println!("ingest: catalog unchanged (304), using cache");
        std::fs::read(&gz_path).with_context(|| format!("reading cached catalog {gz_path}"))?
    } else if status.is_success() {
        // Fresh copy. Persist new ETag + gzip for next run's conditional GET.
        if let Some(tag) = resp.headers().get(reqwest::header::ETAG) {
            if let Ok(tag_str) = tag.to_str() {
                let _ = std::fs::write(&etag_path, tag_str);
            }
        }
        let bytes = resp.bytes().await.context("reading catalog body")?.to_vec();
        std::fs::write(&gz_path, &bytes)
            .with_context(|| format!("caching catalog to {gz_path}"))?;
        println!("ingest: catalog downloaded ({} bytes gz)", bytes.len());
        bytes
    } else {
        return Err(anyhow!("catalog fetch returned {status}"));
    };

    // Decompress gzip -> raw CSV bytes.
    let mut decoder = GzDecoder::new(&gz_bytes[..]);
    let mut csv = Vec::new();
    decoder.read_to_end(&mut csv).context("gunzip catalog")?;
    Ok(csv)
}

// ============================================================
// Parse + filter
// ============================================================

/// Parse the CSV and keep only rows matching the niche filter: correct
/// language, Type == "Text", non-empty title, and a plausibly-old Issued year.
fn parse_and_filter(csv_bytes: &[u8], cfg: &IngestConfig) -> Result<Vec<Book>> {
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true) // PG rows occasionally have ragged trailing fields.
        .from_reader(csv_bytes);

    let mut out = Vec::new();
    for row in rdr.deserialize::<CatalogRow>() {
        let row = match row {
            Ok(r) => r,
            Err(_) => continue, // tolerate the odd malformed line, don't abort
        };

        // Type filter — only the prose, skip "Sound", "Image", "Dataset", etc.
        if !row.kind.eq_ignore_ascii_case("Text") {
            continue;
        }

        // Language filter. PG packs multi-language rows as "en; fr" sometimes.
        let lang_ok = row
            .language
            .split([';', ','])
            .any(|l| l.trim().eq_ignore_ascii_case(&cfg.language));
        if !lang_ok {
            continue;
        }

        let title = row.title.trim();
        if title.is_empty() {
            continue;
        }

        // Issued is YYYY-MM-DD (or sometimes blank). Pull the leading year.
        let issued_year = row.issued.get(0..4).and_then(|y| y.parse::<i32>().ok());

        // Coarse age gate. If we can't read a year, keep it (don't drop on the
        // basis of a parse failure) — the upstream Type/Language filters are
        // the real signal anyway.
        if let Some(year) = issued_year {
            if year > cfg.max_issued_year {
                continue;
            }
        }

        let id: i64 = match row.id.trim().parse() {
            Ok(n) => n,
            Err(_) => continue,
        };

        out.push(Book {
            gutenberg_id: id,
            title: title.to_string(),
            author: clean_author(&row.authors),
            language: cfg.language.clone(),
            issued_year,
            text_url: None,
            body: None,
        });
    }

    Ok(out)
}

/// PG packs authors as "Lastname, Firstname, dates; Second Author, ...".
/// Take the first author, drop the trailing life-dates, flip to "First Last".
fn clean_author(raw: &str) -> String {
    let first = raw.split(';').next().unwrap_or("").trim();
    if first.is_empty() {
        return "Unknown".to_string();
    }
    // "Twain, Mark, 1835-1910" -> parts ["Twain", "Mark", "1835-1910"]
    let parts: Vec<&str> = first.split(',').map(str::trim).collect();
    match parts.as_slice() {
        [last, given, ..] => format!("{given} {last}"),
        [single] => single.to_string(),
        _ => "Unknown".to_string(),
    }
}

// ============================================================
// Per-book text fetch with URL fallbacks
// ============================================================

/// Fetch the plain-text body for a book id, walking known URL patterns. The
/// canonical `-0.txt` (UTF-8) is tried first, then `-8.txt` (latin-1-ish), then
/// the bare `.txt`, then the older `/cache/epub/` path. PG's layout is
/// inconsistent across vintages, so the first 200-with-body wins.
pub async fn fetch_book_text(
    client: &reqwest::Client,
    throttle: &Throttle,
    id: i64,
) -> Result<(String, String)> {
    let candidates = [
        format!("https://www.gutenberg.org/files/{id}/{id}-0.txt"),
        format!("https://www.gutenberg.org/files/{id}/{id}-8.txt"),
        format!("https://www.gutenberg.org/files/{id}/{id}.txt"),
        format!("https://www.gutenberg.org/cache/epub/{id}/pg{id}.txt"),
    ];

    let mut last_err = None;
    for url in candidates {
        // Be polite to PG's servers on bulk runs.
        let _guard = throttle.acquire(PG_HOST).await;

        match client.get(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                let body = resp.text().await.context("reading book text body")?;
                // Guard against PG's HTML 404 page being served as 200 text.
                if body.len() > 1_000 && !body.trim_start().starts_with("<!DOCTYPE") {
                    return Ok((url, body));
                }
            }
            Ok(resp) => last_err = Some(anyhow!("{url} -> {}", resp.status())),
            Err(e) => last_err = Some(anyhow!("{url} -> {e}")),
        }
    }

    Err(last_err.unwrap_or_else(|| anyhow!("no text URL pattern matched for #{id}")))
}

// ============================================================
// PG boilerplate stripping
// ============================================================

/// Cut the Project Gutenberg license header and footer, leaving just the work.
/// PG wraps every text in *** START OF ... *** / *** END OF ... *** markers;
/// we slice between them. Falls back to the raw text if markers are absent
/// (some very old files predate the standard wrapper).
pub fn strip_pg_boilerplate(raw: &str) -> String {
    let start_marker = raw.find("*** START OF").or_else(|| raw.find("***START OF"));
    let end_marker = raw.find("*** END OF").or_else(|| raw.find("***END OF"));

    let body = match (start_marker, end_marker) {
        (Some(s), Some(e)) if e > s => {
            // Advance past the rest of the START marker's own line.
            let after_start = raw[s..].find('\n').map(|nl| s + nl + 1).unwrap_or(s);
            &raw[after_start..e]
        }
        _ => raw, // no standard wrapper — keep everything
    };

    body.trim().to_string()
}

// ============================================================
// SQLite storage
// ============================================================

pub fn book_exists(conn: &rusqlite::Connection, gutenberg_id: i64) -> Result<bool> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(1) FROM books WHERE gutenberg_id = ?1",
        [gutenberg_id],
        |r| r.get(0),
    )?;
    Ok(n > 0)
}

/// Insert one book. Uses INSERT OR IGNORE so a concurrent/retried run can't
/// double-insert on the unique gutenberg_id.
fn insert_book(conn: &mut rusqlite::Connection, book: &Book) -> Result<()> {
    conn.execute(
        "INSERT OR IGNORE INTO books \
         (gutenberg_id, title, author, language, issued_year, text_url, body) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            book.gutenberg_id,
            book.title,
            book.author,
            book.language,
            book.issued_year,
            book.text_url,
            book.body,
        ],
    )?;
    Ok(())
}

// ============================================================
// Title/author search + curated ingest by id
// ============================================================

/// One catalog row surfaced by a search — no text fetched yet.
#[derive(Debug, Clone)]
pub struct CatalogMatch {
    pub gutenberg_id: i64,
    pub title: String,
    pub author: String,
    pub language: String,
    pub issued_year: Option<i32>,
}

/// True when `title` or `author` contains the (already-lowercased) needle.
fn matches_query(title: &str, author: &str, needle_lower: &str) -> bool {
    title.to_lowercase().contains(needle_lower) || author.to_lowercase().contains(needle_lower)
}

/// Pull the leading 4-digit year out of a catalog `Issued` field.
fn issued_year(issued: &str) -> Option<i32> {
    issued.get(0..4).and_then(|y| y.parse::<i32>().ok())
}

/// First language code from PG's (sometimes multi-valued) Language field.
fn first_language(language: &str) -> String {
    language
        .split([';', ','])
        .next()
        .unwrap_or("")
        .trim()
        .to_string()
}

fn catalog_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("yt-pipeline-ingest/0.1 (+contact)")
        .build()
        .context("building catalog http client")
}

/// Search the Gutenberg catalog by case-insensitive title/author substring.
/// Returns up to `limit` Text-type matches in catalog order, regardless of
/// year/language — curation deliberately bypasses the niche's broad filter.
pub async fn search_catalog(
    cfg: &IngestConfig,
    query: &str,
    limit: usize,
) -> Result<Vec<CatalogMatch>> {
    let client = catalog_client()?;
    let csv_bytes = fetch_catalog(&client, cfg)
        .await
        .context("fetching pg_catalog.csv.gz")?;
    Ok(search_rows(&csv_bytes, query, limit))
}

/// Pure search over decompressed catalog CSV bytes (network-free, testable).
fn search_rows(csv_bytes: &[u8], query: &str, limit: usize) -> Vec<CatalogMatch> {
    let needle = query.trim().to_lowercase();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_bytes);

    let mut out = Vec::new();
    for row in rdr.deserialize::<CatalogRow>() {
        let row = match row {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !row.kind.eq_ignore_ascii_case("Text") {
            continue;
        }
        let title = row.title.trim();
        if title.is_empty() {
            continue;
        }
        let author = clean_author(&row.authors);
        if !matches_query(title, &author, &needle) {
            continue;
        }
        let id: i64 = match row.id.trim().parse() {
            Ok(n) => n,
            Err(_) => continue,
        };
        out.push(CatalogMatch {
            gutenberg_id: id,
            title: title.to_string(),
            author,
            language: first_language(&row.language),
            issued_year: issued_year(&row.issued),
        });
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Ingest specific books by Gutenberg id — the curated "I want exactly these"
/// path. Bypasses the year/language filter, fetches text, and stores. Returns
/// the number newly inserted (already-present or not-found ids are skipped).
pub async fn ingest_ids(db_path: &str, cfg: &IngestConfig, ids: &[i64]) -> Result<usize> {
    if ids.is_empty() {
        return Ok(0);
    }
    let client = catalog_client()?;
    let csv_bytes = fetch_catalog(&client, cfg)
        .await
        .context("fetching pg_catalog.csv.gz")?;
    store_ids(db_path, &client, &csv_bytes, ids).await
}

/// Ingest books by exact (case-insensitive) title. Resolves each title to its
/// catalog id(s), then stores like `ingest_ids`. Titles with no exact match are
/// reported and skipped.
pub async fn ingest_titles(db_path: &str, cfg: &IngestConfig, titles: &[String]) -> Result<usize> {
    if titles.is_empty() {
        return Ok(0);
    }
    let client = catalog_client()?;
    let csv_bytes = fetch_catalog(&client, cfg)
        .await
        .context("fetching pg_catalog.csv.gz")?;

    let ids = ids_for_titles(&csv_bytes, titles);
    if ids.is_empty() {
        eprintln!("ingest: no exact title matches for {titles:?}");
        return Ok(0);
    }
    store_ids(db_path, &client, &csv_bytes, &ids).await
}

/// Collect catalog rows for the wanted ids into Books (no text yet). Pure.
fn collect_books(
    csv_bytes: &[u8],
    wanted: &std::collections::HashSet<i64>,
) -> std::collections::HashMap<i64, Book> {
    let mut found = std::collections::HashMap::new();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_bytes);
    for row in rdr.deserialize::<CatalogRow>() {
        let row = match row {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !row.kind.eq_ignore_ascii_case("Text") {
            continue;
        }
        let id: i64 = match row.id.trim().parse() {
            Ok(n) => n,
            Err(_) => continue,
        };
        if !wanted.contains(&id) {
            continue;
        }
        let title = row.title.trim();
        if title.is_empty() {
            continue;
        }
        let lang = first_language(&row.language);
        found.insert(
            id,
            Book {
                gutenberg_id: id,
                title: title.to_string(),
                author: clean_author(&row.authors),
                language: if lang.is_empty() {
                    "en".to_string()
                } else {
                    lang
                },
                issued_year: issued_year(&row.issued),
                text_url: None,
                body: None,
            },
        );
    }
    found
}

/// Resolve exact (case-insensitive, trimmed) titles to catalog ids, in catalog
/// order, de-duplicated. Pure — testable without network.
fn ids_for_titles(csv_bytes: &[u8], titles: &[String]) -> Vec<i64> {
    let wanted: std::collections::HashSet<String> =
        titles.iter().map(|t| t.trim().to_lowercase()).collect();
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::new();
    let mut rdr = csv::ReaderBuilder::new()
        .flexible(true)
        .from_reader(csv_bytes);
    for row in rdr.deserialize::<CatalogRow>() {
        let row = match row {
            Ok(r) => r,
            Err(_) => continue,
        };
        if !row.kind.eq_ignore_ascii_case("Text") {
            continue;
        }
        if !wanted.contains(&row.title.trim().to_lowercase()) {
            continue;
        }
        if let Ok(id) = row.id.trim().parse::<i64>() {
            if seen.insert(id) {
                out.push(id);
            }
        }
    }
    out
}

/// Shared store path: fetch text for each wanted id and insert it. Skips ids
/// already in the library or absent from the catalog. Returns the count stored.
async fn store_ids(
    db_path: &str,
    client: &reqwest::Client,
    csv_bytes: &[u8],
    ids: &[i64],
) -> Result<usize> {
    let throttle = Throttle::from_default();
    let wanted: std::collections::HashSet<i64> = ids.iter().copied().collect();
    let mut found = collect_books(csv_bytes, &wanted);

    let mut conn =
        rusqlite::Connection::open(db_path).with_context(|| format!("opening db {db_path}"))?;

    let mut inserted = 0usize;
    for id in ids {
        let Some(mut book) = found.remove(id) else {
            eprintln!("ingest: id {id} not found in catalog (or not Text type)");
            continue;
        };
        if book_exists(&conn, book.gutenberg_id)? {
            eprintln!("ingest: #{id} already in library, skipping");
            continue;
        }
        match fetch_book_text(client, &throttle, book.gutenberg_id).await {
            Ok((url, body)) => {
                book.text_url = Some(url);
                book.body = Some(strip_pg_boilerplate(&body));
            }
            Err(e) => eprintln!(
                "ingest: text fetch failed for #{} ({}): {e:#}",
                book.gutenberg_id, book.title
            ),
        }
        insert_book(&mut conn, &book)
            .with_context(|| format!("inserting book #{}", book.gutenberg_id))?;
        inserted += 1;
        println!("ingested #{} — {}", book.gutenberg_id, book.title);
    }
    Ok(inserted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_query_title_and_author_case_insensitive() {
        assert!(matches_query("Pride and Prejudice", "Jane Austen", "pride"));
        assert!(matches_query(
            "Pride and Prejudice",
            "Jane Austen",
            "austen"
        ));
        assert!(!matches_query("Moby Dick", "Herman Melville", "pride"));
    }

    #[test]
    fn issued_year_parses_leading_year() {
        assert_eq!(issued_year("1928-05-01"), Some(1928));
        assert_eq!(issued_year(""), None);
    }

    #[test]
    fn first_language_takes_leading_code() {
        assert_eq!(first_language("en; fr"), "en");
        assert_eq!(first_language("de"), "de");
    }

    #[test]
    fn search_rows_filters_by_type_and_query() {
        let csv = "Text#,Type,Issued,Title,Language,Authors\n\
                   1342,Text,1813-01-28,Pride and Prejudice,en,\"Austen, Jane, 1775-1817\"\n\
                   98,Text,1859-04-30,A Tale of Two Cities,en,\"Dickens, Charles, 1812-1870\"\n\
                   9999,Sound,2001-01-01,Pride Audiobook,en,\"Someone\"\n\
                   84,Text,1818-03-11,Frankenstein,en,\"Shelley, Mary, 1797-1851\"\n";
        // matches title (case-insensitive), excludes the non-Text 'Sound' row
        let hits = search_rows(csv.as_bytes(), "pride", 10);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].gutenberg_id, 1342);
        assert_eq!(hits[0].issued_year, Some(1813));

        // matches author across rows
        let by_author = search_rows(csv.as_bytes(), "shelley", 10);
        assert_eq!(by_author.len(), 1);
        assert_eq!(by_author[0].title, "Frankenstein");

        // limit is honored
        let limited = search_rows(csv.as_bytes(), "a", 2);
        assert_eq!(limited.len(), 2);
    }

    const SAMPLE_CSV: &str = "Text#,Type,Issued,Title,Language,Authors\n\
         1342,Text,1813-01-28,Pride and Prejudice,en,\"Austen, Jane, 1775-1817\"\n\
         9999,Sound,2001-01-01,Frankenstein,en,\"Shelley, Mary\"\n\
         84,Text,1818-03-11,Frankenstein,en,\"Shelley, Mary, 1797-1851\"\n";

    #[test]
    fn ids_for_titles_exact_case_insensitive_text_only() {
        // exact title match, case-insensitive; the 'Sound' Frankenstein is excluded
        let ids = ids_for_titles(SAMPLE_CSV.as_bytes(), &["frankenstein".to_string()]);
        assert_eq!(ids, vec![84]);

        // a substring (not exact) does NOT match
        let none = ids_for_titles(SAMPLE_CSV.as_bytes(), &["pride".to_string()]);
        assert!(none.is_empty());

        // multiple titles resolve together
        let both = ids_for_titles(
            SAMPLE_CSV.as_bytes(),
            &[
                "Pride and Prejudice".to_string(),
                "FRANKENSTEIN".to_string(),
            ],
        );
        assert_eq!(both, vec![1342, 84]);
    }

    #[test]
    fn collect_books_only_wanted_text_rows() {
        let wanted: std::collections::HashSet<i64> = [84, 9999].into_iter().collect();
        let found = collect_books(SAMPLE_CSV.as_bytes(), &wanted);
        // 84 is Text and wanted; 9999 is Sound (skipped despite being wanted)
        assert!(found.contains_key(&84));
        assert!(!found.contains_key(&9999));
        assert_eq!(found[&84].title, "Frankenstein");
    }

    #[test]
    fn strips_standard_wrapper() {
        let raw = "license junk\n*** START OF THE PROJECT GUTENBERG EBOOK FOO ***\nReal body here.\n*** END OF THE PROJECT GUTENBERG EBOOK FOO ***\nfooter junk";
        assert_eq!(strip_pg_boilerplate(raw), "Real body here.");
    }

    #[test]
    fn keeps_text_without_markers() {
        let raw = "Ancient file, no wrapper, just prose.";
        assert_eq!(strip_pg_boilerplate(raw), raw);
    }

    #[test]
    fn author_flips_lastname_first() {
        assert_eq!(clean_author("Twain, Mark, 1835-1910"), "Mark Twain");
        assert_eq!(clean_author(""), "Unknown");
    }
}
