package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pulse/pulse-engine/internal/domain"
)

// ClickHouseClient wraps ClickHouse native driver for batch writes.
type ClickHouseClient struct {
	Conn driver.Conn
}

// NewClickHouseClient initializes a native ClickHouse connection pool.
func NewClickHouseClient(ctx context.Context, dsn string) (*ClickHouseClient, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("invalid clickhouse DSN: %w", err)
	}

	opts.MaxOpenConns = 25
	opts.MaxIdleConns = 10
	opts.ConnMaxLifetime = 10 * time.Minute

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open clickhouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &ClickHouseClient{Conn: conn}, nil
}

// WriteRollupsBatch performs an atomic columnar batch insert of computed rollups.
func (c *ClickHouseClient) WriteRollupsBatch(ctx context.Context, rollups []domain.Rollup) error {
	if len(rollups) == 0 {
		return nil
	}

	query := `INSERT INTO pulse.metric_rollups (
		metric_key, window_resolution, window_start, window_end,
		event_count, sum_value, min_value, max_value, mean_value,
		p50_value, p90_value, p99_value, dimensions, created_at
	)`

	batch, err := c.Conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare rollups batch: %w", err)
	}

	for _, r := range rollups {
		err := batch.Append(
			r.MetricKey,
			r.WindowResolution,
			r.WindowStart,
			r.WindowEnd,
			r.EventCount,
			r.SumValue,
			r.MinValue,
			r.MaxValue,
			r.MeanValue,
			r.P50Value,
			r.P90Value,
			r.P99Value,
			r.Dimensions,
			r.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to rollups batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send rollups batch: %w", err)
	}

	return nil
}

// WriteAnomaliesBatch inserts detected anomaly records.
func (c *ClickHouseClient) WriteAnomaliesBatch(ctx context.Context, anomalies []domain.Anomaly) error {
	if len(anomalies) == 0 {
		return nil
	}

	query := `INSERT INTO pulse.metric_anomalies (
		id, event_id, metric_key, timestamp,
		observed_value, expected_mean, expected_stddev,
		score, anomaly_type, severity, dimensions, created_at
	)`

	batch, err := c.Conn.PrepareBatch(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to prepare anomalies batch: %w", err)
	}

	for _, a := range anomalies {
		err := batch.Append(
			a.ID,
			a.EventID,
			a.MetricKey,
			a.Timestamp,
			a.ObservedValue,
			a.ExpectedMean,
			a.ExpectedStdDev,
			a.Score,
			string(a.Type),
			string(a.Severity),
			a.Dimensions,
			a.CreatedAt,
		)
		if err != nil {
			return fmt.Errorf("failed to append to anomalies batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send anomalies batch: %w", err)
	}

	return nil
}

func (c *ClickHouseClient) Close() error {
	return c.Conn.Close()
}
