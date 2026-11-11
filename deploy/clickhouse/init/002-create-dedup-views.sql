CREATE VIEW IF NOT EXISTS miniapm.logs_dedup AS
SELECT DISTINCT ON (event_id) *
FROM miniapm.logs
ORDER BY event_id, ingested_at DESC, event_time DESC;

CREATE VIEW IF NOT EXISTS miniapm.spans_dedup AS
SELECT DISTINCT ON (event_id) *
FROM miniapm.spans
ORDER BY event_id, ingested_at DESC, event_time DESC;
