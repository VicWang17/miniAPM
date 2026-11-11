CREATE TABLE IF NOT EXISTS miniapm.baseline_metrics (
    dataset_id UUID,
    service LowCardinality(String),
    route LowCardinality(String),
    window_start Date,
    window_end Date,
    request_count UInt32,
    error_count UInt32,
    avg_seconds Float64,
    p95_seconds Float64,
    sample_count UInt32,
    generated_at DateTime64(3, 'UTC'),
    engine LowCardinality(String)
) ENGINE = MergeTree
PARTITION BY (dataset_id, window_start)
ORDER BY (dataset_id, service, route, window_start, window_end);
