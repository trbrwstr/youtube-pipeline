// src/bin/oauth_bootstrap.rs
//
// One-shot helper to mint the long-lived refresh token the pipeline's YouTube
// stages (upload, thumbnail, analytics) consume. Runs the OAuth 2.0 loopback
// flow: open the printed consent URL, grant access, and Google redirects back
// to a tiny localhost listener with an authorization code we exchange for a
// refresh token.
//
// The default scopes cover upload + channel management + analytics (incl.
// monetary), so the SAME refresh token works for every stage. Nothing is
// written to disk — the token is printed for you to export.

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use std::io::{Read, Write};
use std::net::TcpListener;

const AUTH_URL: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_URL: &str = "https://oauth2.googleapis.com/token";
const DEFAULT_SCOPES: &str = "https://www.googleapis.com/auth/youtube.upload \
https://www.googleapis.com/auth/youtube \
https://www.googleapis.com/auth/yt-analytics.readonly \
https://www.googleapis.com/auth/yt-analytics-monetary.readonly";

#[derive(Parser, Debug)]
#[command(name = "oauth_bootstrap", about = "Mint a YouTube OAuth refresh token")]
struct Args {
    /// OAuth client id (defaults to $YT_CLIENT_ID).
    #[arg(long, env = "YT_CLIENT_ID")]
    client_id: String,

    /// OAuth client secret (defaults to $YT_CLIENT_SECRET).
    #[arg(long, env = "YT_CLIENT_SECRET")]
    client_secret: String,

    /// Loopback address Google redirects to. Must be registered as an
    /// authorized redirect URI on the OAuth client.
    #[arg(long, default_value = "127.0.0.1:8080")]
    bind: String,

    /// Space-separated OAuth scopes.
    #[arg(long, default_value = DEFAULT_SCOPES)]
    scopes: String,
}

#[derive(serde::Deserialize)]
struct TokenResponse {
    refresh_token: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let redirect = format!("http://{}", args.bind);

    let listener = TcpListener::bind(&args.bind).with_context(|| {
        format!(
            "binding {} — is it free and registered as a redirect URI?",
            args.bind
        )
    })?;

    let consent = format!(
        "{AUTH_URL}?client_id={}&redirect_uri={}&response_type=code\
         &access_type=offline&prompt=consent&scope={}",
        encode(&args.client_id),
        encode(&redirect),
        encode(&args.scopes),
    );

    println!("\n1. Open this URL and grant access:\n\n{consent}\n");
    println!("2. Waiting for the redirect on http://{} ...\n", args.bind);

    let code = wait_for_code(&listener).context("capturing authorization code")?;
    let refresh = exchange_code(&args.client_id, &args.client_secret, &redirect, &code)
        .await
        .context("exchanging code for tokens")?;

    println!("Success! Add this to your environment:\n\n  export YT_REFRESH_TOKEN=\"{refresh}\"\n");
    Ok(())
}

/// Accept one connection, read the redirect request line, pull out `code`.
fn wait_for_code(listener: &TcpListener) -> Result<String> {
    let (mut stream, _) = listener.accept().context("accepting redirect")?;

    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).context("reading redirect request")?;
    let req = String::from_utf8_lossy(&buf[..n]);

    // First line looks like: GET /?code=XXXX&scope=... HTTP/1.1
    let line = req.lines().next().unwrap_or_default();
    let target = line.split_whitespace().nth(1).unwrap_or_default();

    if let Some(code) = query_param(target, "code") {
        let _ = stream.write_all(
            b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\n\
              Authorization received. You can close this tab and return to the terminal.",
        );
        return Ok(code);
    }

    let body = match query_param(target, "error") {
        Some(err) => format!("OAuth error: {err}"),
        None => "No authorization code in redirect".to_string(),
    };
    let _ = stream.write_all(
        format!("HTTP/1.1 400 Bad Request\r\nContent-Type: text/plain\r\n\r\n{body}").as_bytes(),
    );
    Err(anyhow!("{body}"))
}

async fn exchange_code(
    client_id: &str,
    client_secret: &str,
    redirect_uri: &str,
    code: &str,
) -> Result<String> {
    let client = reqwest::Client::new();
    let resp = client
        .post(TOKEN_URL)
        .form(&[
            ("client_id", client_id),
            ("client_secret", client_secret),
            ("code", code),
            ("grant_type", "authorization_code"),
            ("redirect_uri", redirect_uri),
        ])
        .send()
        .await
        .context("posting token exchange")?;

    let status = resp.status();
    let text = resp.text().await.context("reading token response")?;
    if !status.is_success() {
        return Err(anyhow!("token endpoint returned {status}: {text}"));
    }

    let parsed: TokenResponse =
        serde_json::from_str(&text).with_context(|| format!("parsing token response: {text}"))?;
    parsed.refresh_token.ok_or_else(|| {
        anyhow!(
            "no refresh_token in response (was the client already consented? \
             re-run — prompt=consent is set): {text}"
        )
    })
}

/// Extract a query parameter value from a request target like `/?a=1&b=2`.
fn query_param(target: &str, key: &str) -> Option<String> {
    let query = target.split_once('?').map(|(_, q)| q).unwrap_or("");
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=') {
            if k == key {
                return Some(decode(v));
            }
        }
    }
    None
}

/// Percent-encode everything outside the RFC 3986 unreserved set.
fn encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Minimal percent-decode (and '+' → space) for the captured code.
fn decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 3;
                    continue;
                }
                out.push(bytes[i]);
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            c => {
                out.push(c);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}
