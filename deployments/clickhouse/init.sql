CREATE DATABASE IF NOT EXISTS pulse;

CREATE TABLE IF NOT EXISTS pulse.metric_rollups (
    metric_key LowCardinality(String),
    window_resolution LowCardinality(String),
    window_start DateTime64(3, 'UTC'),
    window_end DateTime64(3, 'UTC'),
    event_count UInt64,
    sum_value Float64,
    min_value Float64,
    max_value Float64,
    mean_value Float64,
    p50_value Float64,
    p90_value Float64,
    p99_value Float64,
    dimensions Map(String, String),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(created_at)
PARTITION BY toYYYYMM(window_start)
ORDER BY (metric_key, window_resolution, window_start);

CREATE TABLE IF NOT EXISTS pulse.metric_anomalies (
    id String,
    event_id String,
    metric_key LowCardinality(String),
    timestamp DateTime64(3, 'UTC'),
    observed_value Float64,
    expected_mean Float64,
    expected_stddev Float64,
    score Float64,
    anomaly_type LowCardinality(String),
    severity LowCardinality(String),
    dimensions Map(String, String),
    created_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (metric_key, anomaly_type, timestamp);
