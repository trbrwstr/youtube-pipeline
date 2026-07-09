# YouTube Automation Pipeline for Content Businesses

A production-ready YouTube automation system for creators, agencies, and small businesses that want a repeatable way to turn long-form source material into published videos.

This project was built as a reusable content engine: it can ingest source material, generate short-form hooks and metadata, create narration, assemble finished videos, publish to YouTube, collect analytics, and use performance data to guide the next production cycle.

If you found this repository through Fiverr or Upwork, this is an example of the kind of automation work I can design, build, document, and customize for your workflow.

---

## What This System Does

This pipeline helps automate the repetitive parts of running faceless or semi-automated YouTube channels:

1. **Find or import source content** from approved inputs.
2. **Generate scripts, hooks, titles, descriptions, and tags** using configurable rules and optional AI assistance.
3. **Create narration audio** through a text-to-speech provider.
4. **Assemble finished videos** with captions, visuals, audio, and thumbnails.
5. **Upload videos to YouTube** through the YouTube API.
6. **Collect performance data** such as views, watch time, engagement, and revenue where available.
7. **Adjust future production priorities** based on what performs best.
8. **Run the whole loop hands-free** from a built-in web dashboard: per-channel schedules, batch sizes, publish toggles, a fleet-wide pause switch, and a run history — all server-side, with no cron jobs or client-side tooling.

The goal is not to replace strategy or creativity. The goal is to remove manual bottlenecks so a creator or business can test ideas faster, publish consistently, and make decisions from real performance data.

---

## Who This Is For

This type of system is a good fit for:

- YouTube creators who want to scale production without hiring a large editing team.
- Agencies managing multiple channels or content niches.
- Businesses that want educational, evergreen, or informational video content.
- Operators testing faceless YouTube concepts.
- Teams that already have content assets and need a repeatable publishing workflow.
- Entrepreneurs who want a custom automation backend instead of piecing together disconnected tools.

It is especially useful when the workflow has clear inputs, repeatable formatting rules, and measurable output goals.

---

## Example Use Cases

The current implementation was designed around public-domain book content, but the same architecture can be adapted for many content workflows, including:

- Public-domain stories, classics, summaries, or literary channels.
- Educational explainers.
- News-style briefs from approved internal sources.
- Product education videos.
- Podcast clip repurposing.
- Local business content calendars.
- Training content for teams or customers.
- Multi-channel publishing experiments.

Each niche can have its own configuration, voice, visual style, metadata rules, publishing account, and analytics loop.

---

## Key Benefits

### Repeatable Production

The pipeline follows a clear production chain from content intake to upload. That makes the workflow easier to debug, improve, and delegate.

```text
ingest → script/hook → narration → video assembly → metadata → upload → thumbnail → analytics
```

### Built for Real Operations

The system is designed to be resumable. If a run stops midway, it can continue from the last completed stage instead of starting over.

### Multi-Niche Friendly

Different content brands can share the same engine while using different configuration files, databases, assets, styles, channels, and publishing rules.

### Analytics Feedback Loop

The system can collect YouTube performance data and use it to help decide where future production effort should go. This supports faster testing across niches, formats, and content styles.

### Fully Automated Operation

A built-in web dashboard doubles as the scheduler. Deploy one binary on a server, open the UI, and switch each channel's automation on: the full production chain, daily quota reallocation, and analytics snapshots then run on their own cadence with durable schedules and a complete run history. See [`docs/RUNBOOK.md`](docs/RUNBOOK.md) for the operations guide.

### Practical Automation

This is not a fragile one-off script. It uses a database-backed workflow, stage tracking, retry-friendly design, configurable throttling, and separate modules for each production step.

---

## Project Structure

```text
youtube-pipeline/
├── Cargo.toml              # Rust workspace: library + 6 binaries
├── config/                 # Per-niche TOML configs
│   ├── forgotten_classics.toml
│   └── dead_authors.toml
├── src/
│   ├── lib.rs              # Shared pipeline library
│   ├── bin/                # Standalone executables
│   │   ├── pipeline.rs     # Single-niche stage runner + ops CLI
│   │   ├── orchestrator.rs # Multi-niche fleet driver
│   │   ├── dashboard.rs    # Web dashboard + scheduler
│   │   ├── analytics.rs    # YouTube Analytics snapshot pull
│   │   ├── selector.rs     # Cross-niche budget reallocation
│   │   └── oauth_bootstrap.rs # YouTube OAuth refresh-token helper
│   ├── config.rs           # AppConfig + TOML parsing
│   ├── db.rs               # SQLite schema + migrations
│   ├── state.rs            # Stage tracking, retries, dead letters
│   ├── runner.rs           # Bounded-concurrency stage runner
│   ├── ingest.rs           # Gutenberg / text ingestion
│   ├── hook.rs             # Script / hook generation
│   ├── tts.rs              # Text-to-speech audio
│   ├── assemble.rs         # FFmpeg video assembly + captions
│   ├── metadata.rs         # Title / description / tag generation
│   ├── upload.rs           # YouTube Data API upload
│   ├── thumbnail.rs        # Thumbnail upload after publish
│   ├── analytics.rs        # Performance snapshot logic
│   ├── selector.rs         # Explore/exploit quota allocation
│   ├── automation.rs       # Scheduler + run history
│   └── throttle.rs         # API rate limiting
├── data/                   # SQLite databases + rendered videos
├── cache/                  # Downloaded texts + cached audio
└── docs/RUNBOOK.md         # Operator's guide
```

---

## Quick Start

### 1. Prerequisites

- [Rust](https://rustup.rs/) (stable)
- [FFmpeg](https://ffmpeg.org/download.html) installed and on `$PATH`
- A YouTube OAuth client with the redirect URI `http://127.0.0.1:8080`
- API keys for the services your config uses (OpenAI, TTS, etc.)

### 2. Build

```bash
cargo build --release
```

All binaries are built into `target/release/`.

### 3. Configure

Copy one of the example configs and fill in your credentials:

```bash
cp config/forgotten_classics.toml config/my_channel.toml
```

At minimum you will need to export:

```bash
export OPENAI_API_KEY="sk-..."
export YT_CLIENT_ID="..."
export YT_CLIENT_SECRET="..."
```

### 4. Bootstrap YouTube OAuth

```bash
cargo run --bin oauth_bootstrap
```

Open the printed URL, grant access, then export the resulting refresh token in the matching `YT_REFRESH_TOKEN_*` variable for your niche.

### 5. Run a dry pipeline

Test ingestion and hook generation without uploading:

```bash
cargo run --bin pipeline -- run \
  --config config/my_channel.toml \
  --stages ingest,hook \
  --limit 5
```

### 6. Run the full chain

```bash
cargo run --bin pipeline -- run \
  --config config/my_channel.toml \
  --limit 10
```

To run every configured niche under a shared concurrency cap:

```bash
cargo run --bin orchestrator -- --config-dir config --max-parallel 2
```

### 7. Start the dashboard

```bash
cargo run --bin dashboard -- --port 3000 --config-dir config
```

Open `http://localhost:3000` to view status, trigger runs, enable per-channel automation schedules, and inspect run history.

---

## CLI Reference

### `pipeline`

Single-niche stage runner and operational CLI.

```bash
pipeline run     --config config/forgotten_classics.toml --limit 50
pipeline run     --config config/forgotten_classics.toml --stages hook,tts
pipeline status  --config config/forgotten_classics.toml
pipeline reap    --config config/forgotten_classics.toml --stale-secs 900
pipeline retry   --config config/forgotten_classics.toml --stage upload
pipeline dead    --config config/forgotten_classics.toml
pipeline search  --config config/forgotten_classics.toml "frankenstein" --limit 20
pipeline add     --config config/forgotten_classics.toml --ids 1342,98,84
```

### `orchestrator`

Multi-niche driver that runs every `config/*.toml` in a sweep.

```bash
orchestrator --config-dir config --max-parallel 2
orchestrator --niches forgotten_classics,dead_authors --loop --interval-secs 3600
orchestrator --config-dir config --stages ingest,hook,tts --no-upload
```

### `dashboard`

Web dashboard and scheduler.

```bash
dashboard --port 3000 --config-dir config --state-db data/dashboard.db
```

### `analytics`

Pulls YouTube performance snapshots into the niche's `video_stats` table.

```bash
analytics --config config/forgotten_classics.toml
analytics --config config/forgotten_classics.toml --until-days-ago 2
```

Run after you have published videos; the selector uses these numbers to reallocate quota.

### `selector`

Allocates the next production cycle's budget across niches.

```bash
selector --config config/forgotten_classics.toml
selector --config-dir config
```

Use `--config-dir` for the federated cross-channel mode.

### `oauth_bootstrap`

Mint a long-lived refresh token for the YouTube upload / analytics stages.

```bash
oauth_bootstrap --client-id "$YT_CLIENT_ID" --client-secret "$YT_CLIENT_SECRET"
```

---

## Services I Can Provide

I can help clients with work such as:

- Building a custom YouTube automation pipeline from scratch.
- Adapting this engine for a specific niche, brand, or content format.
- Connecting APIs such as OpenAI, YouTube, TTS providers, analytics tools, and internal systems.
- Designing video templates, metadata workflows, and production rules.
- Creating dashboards or reports for content performance.
- Automating uploads, thumbnails, captions, descriptions, and tags.
- Hardening an existing automation system for reliability and security.
- Refactoring messy scripts into maintainable production tools.
- Writing documentation and runbooks so a non-technical operator can use the system.

If your workflow is repetitive, data-driven, and has clear rules, it can likely be automated or partially automated.

---

## Typical Client Workflow

A client engagement usually follows this structure:

1. **Discovery**
   - Understand your content goals, channels, inputs, publishing cadence, and constraints.

2. **Workflow Design**
   - Map the production process from raw input to final upload.
   - Identify what should be automated and what should stay human-reviewed.

3. **MVP Build**
   - Build the smallest reliable version that proves the workflow end to end.

4. **Testing and Review**
   - Validate output quality, API connections, edge cases, and operational steps.

5. **Iteration**
   - Improve quality, speed, prompts, templates, analytics, and reliability.

6. **Documentation and Handoff**
   - Provide setup notes, commands, environment variables, and operating instructions.

---

## Technology Stack

This repository is primarily built in **Rust** for reliability, performance, and maintainability. It uses a modular architecture with separate components for configuration, database access, content ingestion, script generation, narration, video assembly, upload, analytics, and production planning.

Common integrations include:

- YouTube Data API
- YouTube Analytics API
- OpenAI-compatible APIs
- Text-to-speech providers
- FFmpeg
- SQLite
- TOML configuration
- Command-line automation

The exact stack can be adjusted based on the client's hosting environment, budget, and technical comfort level.

---

## What Can Be Customized

Nearly every part of the system can be tailored:

- Content source and filtering rules
- Script format and tone
- Voice provider and narration style
- Video dimensions and templates
- Background visuals and branding
- Caption styling
- Thumbnail generation workflow
- Title, description, and tag rules
- Upload schedule and channel mapping
- Analytics metrics and scoring logic
- Human review checkpoints
- Error handling and retry behavior
- Hosting and deployment approach

---

## Important Notes

- This project does **not** guarantee views, subscribers, monetization, or revenue.
- YouTube results depend on niche, packaging, content quality, retention, consistency, competition, and audience fit.
- Any production system should respect copyright, platform terms, API limits, privacy obligations, and applicable laws.
- Human review is recommended before publishing, especially for regulated, sensitive, copyrighted, or brand-critical content.
- API costs, TTS costs, hosting costs, and YouTube account requirements are the client's responsibility unless otherwise agreed.

---

## Repository Status

This repository demonstrates a working automation architecture and can be used as a foundation for custom client work. Production deployments should be configured for the client's own accounts, API keys, content sources, brand requirements, and compliance needs.

For a full internal operations guide, see [`docs/RUNBOOK.md`](docs/RUNBOOK.md).

---

## Contact / Hiring

I am available for freelance projects involving automation, backend systems, API integrations, workflow tools, and content production pipelines.

When reaching out, please include:

- A short description of your content workflow or business process.
- The platforms and APIs you want to use.
- Your desired output format.
- Your expected publishing volume or automation goal.
- Any existing tools, scripts, or assets you already have.

The more clearly the workflow is defined, the faster we can build a useful first version.

---

## Legal Notice

Copyright © 2026. All rights reserved.

All source code, documentation, architecture, workflows, designs, prompts, configuration examples, and related materials in this repository are protected by copyright and other applicable intellectual property laws. No part of this repository may be copied, reproduced, modified, distributed, published, sublicensed, sold, or used to create derivative works without prior written permission from the copyright owner, except where a separate written license or agreement expressly allows it.

This repository is provided for portfolio, demonstration, evaluation, and authorized client work purposes only. Access to this repository does not grant ownership, commercial usage rights, resale rights, redistribution rights, or any implied license.

All third-party trademarks, service marks, APIs, platforms, and brand names referenced in this repository remain the property of their respective owners. Users are responsible for complying with all applicable platform terms, API policies, copyright laws, privacy laws, and other legal obligations.

This software and documentation are provided “as is” without warranties of any kind, express or implied, including but not limited to warranties of merchantability, fitness for a particular purpose, non-infringement, availability, accuracy, or uninterrupted operation. The copyright owner is not liable for any direct, indirect, incidental, consequential, special, exemplary, or punitive damages arising from use of, reliance on, or inability to use this repository or any related system.
