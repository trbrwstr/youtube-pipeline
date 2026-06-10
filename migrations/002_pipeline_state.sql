-- Per-book stage tracking. One row per (book_id, stage), so the orchestrator
-- reads explicit status instead of inferring it from IS NULL joins. This is
-- what the resume logic and the dead-letter queue both hang off of.

CREATE TABLE IF NOT EXISTS pipeline_state (
    book_id      INTEGER NOT NULL,
    stage        TEXT    NOT NULL,   -- ingest|hook|tts|assemble|metadata|thumbnail|upload
    status       TEXT    NOT NULL    -- pending|running|done|failed|dead
                 DEFAULT 'pending'
                 CHECK (status IN ('pending','running','done','failed','dead')),
    attempts     INTEGER NOT NULL DEFAULT 0,
    started_at   TEXT,               -- ISO-8601, set when status -> running
    updated_at   TEXT    NOT NULL DEFAULT (datetime('now')),
    error        TEXT,               -- last failure message, NULL on success
    meta         TEXT,               -- optional JSON blob (durations, costs, ids)

    PRIMARY KEY (book_id, stage),
    FOREIGN KEY (book_id) REFERENCES books(id) ON DELETE CASCADE
);

-- The hot query: "give me the next batch needing stage X."
CREATE INDEX IF NOT EXISTS idx_state_stage_status
    ON pipeline_state (stage, status);

-- Dead-letter sweep: "what's permanently stuck?"
CREATE INDEX IF NOT EXISTS idx_state_dead
    ON pipeline_state (status) WHERE status = 'dead';