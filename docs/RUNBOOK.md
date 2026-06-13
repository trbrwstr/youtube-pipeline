# Operations Runbook

How to run the engine for real, across **multiple channels/niches**. For the
architecture and module map, see the top-level [README](../README.md).

One niche = one TOML in `config/` = one SQLite DB = (usually) one YouTube
channel. The code path is identical for every niche; they differ only by config
and visual template.

---

## 1. Prerequisites

- **Rust** 1.75+ and a C toolchain (`rusqlite` builds bundled SQLite from source).
- **ffmpeg** + **ffprobe** on `PATH` (the `assemble` stage shells out to them).
- **A TTS provider** with an OpenAI-compatible `/audio/speech` endpoint.
- **An LLM endpoint** for hooks/metadata (OpenAI-compatible `/chat/completions`).
  Both degrade gracefully: a failing LLM call falls back to the deterministic
  builder, so the pipeline never hard-stops on the text stages.
- **A Google Cloud project** with the **YouTube Data API v3** and **YouTube
  Analytics API** enabled, plus an **OAuth client** (type: Desktop / "Web" with
  a loopback redirect URI `http://127.0.0.1:8080`).
- For revenue numbers: each channel must be in the **YouTube Partner Program**.

---

## 2. Credentials — one app, one token per channel

The OAuth **app** (client id + secret) is shared across all your channels. The
**refresh token is per channel**: you mint one while signed into each channel.

```bash
export YT_CLIENT_ID="...apps.googleusercontent.com"
export YT_CLIENT_SECRET="..."

# Run once PER CHANNEL. A consent URL prints; open it signed into the channel
# you want this niche to publish to, approve, and copy the exported token.
cargo run --bin oauth_bootstrap            # -> export YT_REFRESH_TOKEN_...="1//0g..."
```

The default scopes cover upload + channel management + analytics (incl.
monetary), so the same token works for `upload`, `thumbnail`, and `analytics`.

Export each channel's token under the variable name its config references:

```bash
export YT_REFRESH_TOKEN_CLASSICS="1//0g...classics"
export YT_REFRESH_TOKEN_DEAD_AUTHORS="1//0g...deadauthors"
export OPENAI_API_KEY="sk-..."             # shared LLM/TTS key (or per-niche)
```

`${VAR}` references in the TOML are resolved at config load; a missing/empty
required var is a hard error up front, not a 3am surprise in `upload`.

---

## 3. Per-niche config

Each `config/<niche>.toml` is a full `AppConfig`. The pieces that make a niche
distinct:

```toml
db_path = "./data/<niche>.db"          # isolated DB per niche

[channel]
niche  = "<stable-key>"                 # what selector keys quotas on
format = "long"                         # or "short"

[auth]
refresh_token = "${YT_REFRESH_TOKEN_<NICHE>}"   # THIS channel's token

[tts]
voice = "onyx"                          # per-niche narrator voice

[assemble]
background_image  = "assets/<niche>_bg_1080x1920.png"
font_size         = 58                  # visual template — all optional
font_color        = "white"
box_color         = "black@0.55"
caption_bottom_px = 220
# font_file       = "assets/fonts/Yourfont.ttf"

[selector]                              # keep IDENTICAL across niches in
total_budget   = 100                    # federated mode — the budget is global
min_per_niche  = 5
max_per_niche  = 60
min_sample     = 10
exploit_weight = 0.7
```

Drop a 1080×1920 still per niche under `assets/`. The orchestrator discovers
every `config/*.toml` automatically.

---

## 4. The daily loop

```bash
# 1. PRODUCE — walk every niche's stage chain, gated by each niche's quota.
cargo run --release --bin orchestrator -- --config-dir config --max-parallel 2

#    (or one niche at a time, with the ops CLI)
# cargo run --release --bin pipeline -- run --config config/forgotten_classics.toml

# 2. MEASURE — pull a fresh performance snapshot per channel into video_stats.
#    Run once per channel (each uses its own token / channel==MINE).
for n in forgotten_classics dead_authors; do
  cargo run --release --bin analytics -- --config config/$n.toml
done

# 3. REALLOCATE — ONE shared budget across all niches (cross-channel).
#    Writes each niche's quota back into its own DB.
cargo run --release --bin selector -- --config-dir config

# 4. Next produce sweep reads its batch size from quota_for() automatically.
```

Put steps 1–3 on a daily cron, or run the orchestrator with `--loop
--interval-secs 86400`.

### How the feedback loop reallocates across channels

`analytics` writes `video_stats` per channel. `selector --config-dir` reads
**every** niche's latest snapshot, scores each `(niche, format)` by
revenue-per-video, blends an even *explore* split with a score-weighted
*exploit* split (`exploit_weight`), clamps to `[min_per_niche, max_per_niche]`,
and writes each niche's share back to its own `production_plan`. Niches under
`min_sample` videos are held on the explore track so noisy early numbers can't
starve an unproven channel. `ingest`/`hook` then read `quota_for()` and produce
toward what pays. The one knob you tune over time is `exploit_weight` (start
0.7; lower it if the budget thrashes, raise it to milk proven winners).

---

## 5. Adding a new channel/niche

1. `cp config/forgotten_classics.toml config/<niche>.toml`.
2. Set `db_path`, `[channel].niche/format`, and the `[auth].refresh_token` var.
3. `oauth_bootstrap` signed into the new channel; export its token under that var.
4. Add `assets/<niche>_bg_1080x1920.png` and tune the `[assemble]` template.
5. Keep `[selector]` identical to the other niches (the budget is global).
6. The next orchestrator sweep picks it up — no code changes.

---

## 5a. Curating specific titles

`ingest` pulls the catalog broadly (English, `max_issued_year`, …). To target a
specific book instead, search the Gutenberg catalog by title/author and ingest
the matches you pick — this bypasses the year filter, so you can grab any title.

```bash
# Search the catalog (read-only): prints id, year, lang, title, author,
# and marks anything already in this niche's library.
pipeline search "frankenstein" --config config/forgotten_classics.toml
pipeline search "austen" --limit 100

# Ingest the ones you want — by Gutenberg id, by exact title, or both:
pipeline add --config config/forgotten_classics.toml --ids 84,1342,98
pipeline add --config config/forgotten_classics.toml --titles "Frankenstein;Dracula"

# Or ingest every match from a search in one shot:
pipeline search "h. g. wells" --ingest --limit 25

# Then produce as usual — the curated books flow through the same chain.
pipeline run --config config/forgotten_classics.toml --stages hook,tts,assemble,metadata
```

Notes:
- The first `search`/`add` downloads the ~80MB catalog (then it's ETag-cached).
- `search --ingest` ingests every listed match (capped by `--limit`).
- `add --titles` matches the **exact** title (case-insensitive); use `search`
  first if you're unsure of the precise title. `--ids` and `--titles` combine.
- `add` skips books already in the library and reports any id/title not found.
- Curated books ignore `max_issued_year`/`language` — you asked for them.

---

## 6. Dry run without live services

Stages degrade gracefully, so you can exercise the front of the chain offline:

```bash
export YT_CLIENT_ID=dummy YT_CLIENT_SECRET=dummy OPENAI_API_KEY=dummy
export YT_REFRESH_TOKEN_CLASSICS=dummy YT_REFRESH_TOKEN_DEAD_AUTHORS=dummy

cargo run --bin pipeline -- run --config config/forgotten_classics.toml \
  --stages ingest,hook --no-upload        # hooks fall back to deterministic
cargo run --bin pipeline -- status --config config/forgotten_classics.toml
```

`tts`/`assemble` need a real TTS endpoint + ffmpeg; `upload`/`thumbnail`/
`analytics` need live Google APIs. The pure feedback-loop logic
(`selector::allocate`) is covered by `cargo test`.

---

## 7. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `missing/empty required environment variable(s)` | a `${VAR}` isn't exported | export the per-channel token the config names |
| `invalid_grant` on upload/analytics | refresh token revoked / wrong channel | re-run `oauth_bootstrap` signed into the right channel |
| `thumbnails.set 403` | channel not phone-verified | verify the channel in YouTube Studio |
| Revenue all `$0.00` | not in YPP, or window too recent | confirm monetization; analytics ends the window at `now-2d` |
| One niche starved to its floor | `min_sample` not met, or genuinely low RPM | it's on the explore track until `min_sample` videos exist |
| `selector` budget looks off in federated mode | niches have differing `[selector]` blocks | keep them identical; the budget is global (adopted from the first niche) |
| Items stuck `running` after a crash | worker died mid-stage | `pipeline reap --config <niche> --stale-secs 900` |
| Inspect failures | exhausted retries | `pipeline dead --config <niche>` then `pipeline retry --config <niche> --stage <stage>` |
