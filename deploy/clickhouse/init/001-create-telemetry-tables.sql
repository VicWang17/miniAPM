CREATE DATABASE IF NOT EXISTS miniapm;

CREATE TABLE IF NOT EXISTS miniapm.logs (
    event_id UUID,
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    environment LowCardinality(String),
    dataset_id UUID,
    service LowCardinality(String),
    instance String,
    version LowCardinality(String),
    route String,
    source_kind LowCardinality(String),
    trace_id FixedString(32) DEFAULT '',
    span_id FixedString(16) DEFAULT '',
    severity LowCardinality(String),
    message String,
    error_code Nullable(String)
)
ENGINE = MergeTree
PARTITION BY toDate(event_time)
ORDER BY (service, event_time, trace_id, event_id)
TTL event_time + INTERVAL 7 DAY;

CREATE TABLE IF NOT EXISTS miniapm.spans (
    event_id UUID,
    event_time DateTime64(3, 'UTC'),
    ingested_at DateTime64(3, 'UTC'),
    environment LowCardinality(String),
    dataset_id UUID,
    service LowCardinality(String),
    instance String,
    version LowCardinality(String),
    route String,
    source_kind LowCardinality(String),
    trace_id FixedString(32),
    span_id FixedString(16),
    parent_span_id Nullable(FixedString(16)),
    kind Enum8('server' = 1, 'client' = 2, 'internal' = 3),
    started_at DateTime64(3, 'UTC'),
    ended_at DateTime64(3, 'UTC'),
    status Enum8('ok' = 1, 'error' = 2)
)
ENGINE = MergeTree
PARTITION BY toDate(started_at)
ORDER BY (service, started_at, trace_id, span_id)
TTL started_at + INTERVAL 7 DAY;
