package domain

import (
	"errors"
	"strings"
	"time"
)

var (
	ErrEmptyEventID     = errors.New("event_id cannot be empty")
	ErrEmptyMetricKey   = errors.New("metric_key cannot be empty")
	ErrZeroTimestamp    = errors.New("timestamp cannot be zero")
	ErrFutureTimestamp  = errors.New("timestamp exceeds allowed future clock skew")
	ErrExpiredTimestamp = errors.New("timestamp exceeds allowed past clock skew")
)

// Event represents a canonical time-series data point ingested into Pulse.
type Event struct {
	// EventID uniquely identifies this specific event instance (UUIDv4).
	// Used as the primary key for Redis deduplication.
	EventID string `json:"event_id" db:"event_id"`

	// MetricKey defines the logical time series (e.g., "retail.orders.count", "api.latency.p99").
	// Redpanda uses this as the partition key to guarantee single-worker ownership per series.
	MetricKey string `json:"metric_key" db:"metric_key"`

	// Timestamp is the occurrence time in UTC (RFC3339).
	Timestamp time.Time `json:"timestamp" db:"timestamp"`

	// Value is the measured float64 value (e.g., count, latency in ms, currency amount).
	Value float64 `json:"value" db:"value"`

	// Dimensions provide categorical key-value metadata for filtering and cold-start clustering.
	Dimensions map[string]string `json:"dimensions,omitempty" db:"dimensions"`
}

// Validate ensures structural validity and clock-skew bounds against server time.
func (e *Event) Validate(now time.Time, maxFutureSkew, maxPastSkew time.Duration) error {
	if strings.TrimSpace(e.EventID) == "" {
		return ErrEmptyEventID
	}
	if strings.TrimSpace(e.MetricKey) == "" {
		return ErrEmptyMetricKey
	}
	if e.Timestamp.IsZero() {
		return ErrZeroTimestamp
	}

	if maxFutureSkew > 0 && e.Timestamp.After(now.Add(maxFutureSkew)) {
		return ErrFutureTimestamp
	}
	if maxPastSkew > 0 && e.Timestamp.Before(now.Add(-maxPastSkew)) {
		return ErrExpiredTimestamp
	}

	return nil
}

// Batch represents a collection of events ingested together.
type Batch struct {
	Events []Event `json:"events"`
}
