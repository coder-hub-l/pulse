package domain

import (
	"time"
)

// Rollup represents an aggregated window of metrics computed in the streaming pipeline.
type Rollup struct {
	MetricKey        string            `json:"metric_key" db:"metric_key"`
	WindowResolution string            `json:"window_resolution" db:"window_resolution"` // "1m", "5m", "1h", "1d"
	WindowStart      time.Time         `json:"window_start" db:"window_start"`
	WindowEnd        time.Time         `json:"window_end" db:"window_end"`
	EventCount       uint64            `json:"event_count" db:"event_count"`
	SumValue         float64           `json:"sum_value" db:"sum_value"`
	MinValue         float64           `json:"min_value" db:"min_value"`
	MaxValue         float64           `json:"max_value" db:"max_value"`
	MeanValue        float64           `json:"mean_value" db:"mean_value"`
	P50Value         float64           `json:"p50_value" db:"p50_value"`
	P90Value         float64           `json:"p90_value" db:"p90_value"`
	P99Value         float64           `json:"p99_value" db:"p99_value"`
	Dimensions       map[string]string `json:"dimensions,omitempty" db:"dimensions"`
	CreatedAt        time.Time         `json:"created_at" db:"created_at"`
}
