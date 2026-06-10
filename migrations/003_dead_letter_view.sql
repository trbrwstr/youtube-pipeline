-- Human-readable dead-letter joining book titles onto the state rows.
CREATE VIEW IF NOT EXISTS v_dead_letters AS
SELECT
    ps.book_id,
    b.title,
    b.author,
    ps.stage,
    ps.attempts,
    ps.last_error,
    datetime(ps.updated_at, 'unixepoch') AS failed_at
FROM pipeline_state ps
JOIN books b ON b.id = ps.book_id
WHERE ps.status = 'dead'
ORDER BY ps.updated_at DESC;