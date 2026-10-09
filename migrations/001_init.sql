-- Pulse Initial Schema

CREATE TABLE IF NOT EXISTS metric_rollups (
    metric_key VARCHAR(255) NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    window_end TIMESTAMPTZ NOT NULL,
    event_count BIGINT NOT NULL,
    sum_value DOUBLE PRECISION NOT NULL,
    min_value DOUBLE PRECISION NOT NULL,
    max_value DOUBLE PRECISION NOT NULL,
    mean_value DOUBLE PRECISION NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (metric_key, window_start)
);

CREATE INDEX IF NOT EXISTS idx_metric_rollups_window ON metric_rollups (metric_key, window_start DESC);

CREATE TABLE IF NOT EXISTS metric_anomalies (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255),
    metric_key VARCHAR(255) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    baseline_mean DOUBLE PRECISION NOT NULL,
    baseline_stddev DOUBLE PRECISION NOT NULL,
    z_score DOUBLE PRECISION NOT NULL,
    anomaly_type VARCHAR(64) NOT NULL,
    dimensions JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_metric_anomalies_time ON metric_anomalies (metric_key, timestamp DESC);
