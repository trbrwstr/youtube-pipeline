# YouTube Automation Pipeline — Melampus Parity Plan

Status: proposed execution plan  
Primary motion: creator/agency automation IP, license, and implementation services  
Initial wedge: rights-cleared, human-approved production workflow for one channel format

YouTube Pipeline is close to Melampus technically: it has a Rust workflow engine, durable state, retries/dead letters, scheduling, OAuth, throttling, analytics, a dashboard, CI, and an operations runbook. Parity requires enforceable content-rights and approval gates, golden media/e2e evidence, cost and platform-policy controls, secure recovery, and a reference client outcome.

## Project Charter

### Agent execution contract

- Read `README.md`, `Cargo.toml`, `.github/workflows/ci.yml`, `docs/RUNBOOK.md`, workflow/database/config/throttle/ingest modules, OAuth/upload paths, and dashboard/scheduler code before editing.
- Execute YTP-001 before adding providers, niches, publishing features, or optimization agents.
- No content may publish without an attributable human approval for the exact immutable media artifact and metadata revision.
- Every source asset must carry rights/provenance and permitted-use metadata. Unknown rights block rendering/publication.
- Keep provider adapters replaceable. Script/TTS/media failures retain editable intermediate state and support deterministic/manual recovery.
- Work one task ID per PR. Workflow/schema changes require migration, idempotency, retry, and dead-letter tests.
- Use test channels or mocked upload boundaries in CI; never publish during automated verification.
- PRs must list tasks, commands, golden-media diffs, policy/security impact, cost impact, migrations, and rollback.

### Product definition

### Problem, user, and buyer

Creators and agencies repeatedly coordinate research/source assets, scripts, narration, rendering, metadata, upload, scheduling, and analytics across fragile tools. Automation can also amplify copyright violations, spam, misinformation, and accidental publication. The primary user is a channel operator/editor; the buyer is a creator business or agency with one repeatable, rights-cleared format.

### Product thesis and sellable wedge

The wedge is an approval-centered production pipeline: register rights-cleared inputs, draft and edit a script, generate or attach narration, render a reproducible media candidate, run policy/quality checks, approve the exact revision, upload to a test or production channel, and ingest analytics. Durable value comes from resumable orchestration, provenance, approval, cost visibility, and reliable recovery—not mass unattended publishing.

### Data rights, ethics, and AI resilience

Customers own channel credentials, source inventory, scripts, rendered media, approvals, and analytics subject to platform and source terms. Store minimum OAuth scopes and support credential revocation/export/deletion. Exclude impersonation, deceptive synthetic media, unlicensed reuse, engagement manipulation, and bypassing platform enforcement. Manual scripts, uploaded narration, and manual media remain possible when AI providers fail.

### Commercial proof and kill criteria

Sell setup/customization plus a license or managed operation. Measure production time, cost per approved asset, failure/recovery, approval edits, rejected policy checks, publishing accuracy, and channel outcome without promising revenue. Narrow or park if compliant human review removes no meaningful operational cost or if the target format lacks defensible source rights.

## Implementation plan

### Architecture and data

Retain the Rust modular workflow and database-backed state. Keep source, script, TTS, render, YouTube, analytics, and optional selection providers behind adapters. Core entities are workspace/channel, credential reference, source asset/version, rights record, script revision, voice/audio asset, render recipe, media artifact/hash, quality/policy check, approval, publication job/attempt, platform asset, analytics snapshot, cost event, audit event, retention policy, and deletion job.

### Ordered task backlog

| ID | Priority | Work | Acceptance evidence |
|---|---|---|---|
| YTP-001 | P0 | Select one channel format/buyer and document allowed sources, prohibited content, mandatory review points, outcome metrics, and exclusions. | ADR, demo configuration, and sample fixtures use one compliant format and no autonomous-publishing claim. |
| YTP-002 | P0 | Add versioned rights/provenance schema for every source, voice, music, image, clip, script input, and generated asset. | Missing/expired/incompatible rights fail closed before render/publication; export traces every component. |
| YTP-003 | P0 | Make scripts, metadata, render recipes, and media artifacts immutable revisions with hashes and lineage. | Any published asset maps to exact inputs, providers/versions, checks, approval, and uploader. |
| YTP-004 | P0 | Enforce human approval for the exact media+metadata revision with re-approval after changes and optional two-person control. | API/UI/CLI tests prove stale approval cannot publish and every publication has attributable approval. |
| YTP-005 | P0 | Add deterministic policy/quality gate framework for missing rights, prohibited terms, disclosure requirements, duration/audio/video constraints, duplicates, and configured platform rules. | Golden fixtures produce explainable pass/fail/needs-review without relying solely on an LLM. |
| YTP-006 | P0 | Harden OAuth/token storage, minimum scopes, CSRF/state, rotation/revocation, test-versus-production channel separation, and redacted logging. | Security tests cover callback forgery, expired/revoked tokens, wrong channel/environment, and secret leakage. |
| YTP-007 | P0 | Add idempotent publication state machine covering resumable upload, duplicate prevention, partial failure, retry budgets, dead letters, cancellation, and reconciliation. | Replayed/concurrent jobs create at most one platform asset and reconcile unknown outcomes safely. |
| YTP-008 | P0 | Create rights-cleared golden project fixtures and reproducible media tests for script → audio → FFmpeg render → checks → approval → mocked/test upload → analytics. | CI verifies hashes or tolerance-based media properties and complete lineage without public publishing. |
| YTP-009 | P0 | Instrument per-job/provider cost, runtime, retry, approval-edit, rejection, and throughput with configurable budgets/circuit breakers. | A job exceeding budget pauses visibly before incurring additional provider cost. |
| YTP-010 | P0 | Extend CI/release with format/clippy/test, migration tests, golden media, adapter contracts, security checks, dependency/license review, SBOM, signed artifacts, and install smoke. | Candidate release is reproducible and verified from a clean environment. |
| YTP-011 | P0 | Add encrypted backup/restore, media/object reconciliation, health/readiness, queue/dead-letter dashboards, release rollback, credential incident, and platform outage procedures. | Recovery rehearsal preserves lineage/approvals and never republishes completed work. |
| YTP-012 | P1 | Add export/deletion/retention controls across database, media, temp files, provider references, tokens, and analytics. | Verified deletion and channel offboarding produce a complete manifest and revoke credentials. |
| YTP-013 | P1 | Produce architecture/data flow, threat/policy model, rights guide, provider/license inventory, cost model, claims ledger, demo runbook, and diligence index. | Buyer can reproduce the workflow, cost, ownership, and release evidence. |
| YTP-014 | P1 | Run one reference-client pilot and publish a redacted evidence packet covering time, cost, failures, approvals, and policy blocks. | Evidence separates operational improvement from channel-performance speculation. |
| YTP-015 | P2 | Add another format/provider/channel only after the pilot identifies it as a buying blocker. | Expansion reuses rights, lineage, approval, cost, and recovery primitives. |

### Security and operations

Threat-model OAuth theft, accidental/wrong-channel publication, duplicate uploads, unlicensed sources, prompt injection from ingested content, malicious media, FFmpeg/resource abuse, provider supply-chain failure, cost runaway, platform-policy drift, and incomplete deletion. Use minimum scopes, encrypted secrets, immutable approvals, bounded processors, safe argument construction, explicit environment separation, budget breakers, integrity hashes, and reconciliation before retry.

### Verification commands

Use the pinned Rust toolchain and existing CI/runbook. Minimum evidence includes `cargo fmt --check`, configured `cargo clippy`, `cargo test`, database migration/recovery tests, adapter contract tests, golden FFmpeg/media checks, OAuth/security tests, idempotency/reconciliation tests, SBOM/license generation, signed release verification, and a test-channel smoke with manual authorization. Automated tests must never publish publicly.

## MVP milestones

### M0 — Format and rights contract

- **Outcome:** one compliant production format and its source rules are authoritative.
- **Deliverables:** YTP-001 and YTP-002.
- **Dependencies:** buyer/source-rights hypothesis.
- **Exit gate:** every fixture/source has machine-enforced rights metadata.
- **Deferred:** multi-niche expansion.

### M1 — Approval-centered money path

- **Outcome:** a rights-cleared project becomes one safely published, traceable asset.
- **Deliverables:** YTP-003 through YTP-009.
- **Dependencies:** M0.
- **Exit gate:** stale-approval, duplicate, provider failure, budget, golden-media, wrong-channel, and reconciliation tests pass.
- **Deferred:** autonomous optimization/publishing.

### M2 — Releasable and recoverable asset

- **Outcome:** the pipeline installs, upgrades, restores, offboards, and supports buyer diligence.
- **Deliverables:** YTP-010 through YTP-013.
- **Dependencies:** M1.
- **Exit gate:** clean candidate plus restore/rollback/offboarding and policy review passes.
- **Deferred:** hosted multi-tenant platform.

### M3 — Reference client evidence

- **Outcome:** real operations validate the service/license motion.
- **Deliverables:** YTP-014.
- **Dependencies:** M2.
- **Exit gate:** redacted evidence demonstrates operational value and informs any YTP-015 expansion.
- **Deferred:** YTP-015 until the gate passes.

### Next three actions

1. Execute YTP-001 by freezing one buyer/channel format and its policy/approval boundaries.
2. Execute YTP-002 by inventorying every current asset/provider path and making unknown rights a blocking state.
3. Capture current CI/runbook, OAuth, publication, recovery, and cost baselines before changing the workflow state machine.
