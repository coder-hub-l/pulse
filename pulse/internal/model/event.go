package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Event represents a single metric measurement from a producer.
// EventID is client-generated (UUID) to support idempotent retries.
// Timestamp is event time (when the measurement happened), not ingestion time.
// Value is float64 (not *float64): zero is a valid measurement; silence is
// detected by absence of events, never by a null or zero value.
type Event struct {
	EventID    string            `json:"event_id"`
	MetricKey  string            `json:"metric_key"`
	Timestamp  time.Time         `json:"timestamp"`
	Value      float64           `json:"value"`
	Dimensions map[string]string `json:"dimensions"`
}

// DimensionKey returns a stable string key for the metric_key + dimensions
// combination. Used to group events into the same rollup bucket and baseline.
// Dimensions are sorted by key to ensure deterministic ordering.
func (e *Event) DimensionKey() string {
	if len(e.Dimensions) == 0 {
		return e.MetricKey
	}

	// Build sorted dimension pairs. We iterate twice to avoid importing "sort"
	// for a small map: first collect keys, then insertion-sort them.
	keys := make([]string, 0, len(e.Dimensions))
	for k := range e.Dimensions {
		keys = append(keys, k)
	}
	// Insertion sort — dimension maps are small (typically <10 keys).
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}

	var b strings.Builder
	b.WriteString(e.MetricKey)
	for _, k := range keys {
		b.WriteByte('|')
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(e.Dimensions[k])
	}
	return b.String()
}

// Validate checks all invariants on an Event. Returns a combined error
// describing every violated rule, not just the first one, so the client
// can fix all problems in one round-trip.
func (e *Event) Validate() error {
	var errs []string

	// EventID must be a valid UUID.
	if _, err := uuid.Parse(e.EventID); err != nil {
		errs = append(errs, fmt.Sprintf("event_id: must be a valid UUID, got %q", e.EventID))
	}

	// MetricKey must be non-empty and bounded.
	if e.MetricKey == "" {
		errs = append(errs, "metric_key: must not be empty")
	} else if len(e.MetricKey) > 256 {
		errs = append(errs, fmt.Sprintf("metric_key: must be <= 256 chars, got %d", len(e.MetricKey)))
	}

	// Timestamp must not be zero (i.e., the field must be present).
	if e.Timestamp.IsZero() {
		errs = append(errs, "timestamp: must not be zero/missing")
	}

	// Value: NaN and ±Inf are not valid measurements.
	if e.Value != e.Value { // NaN check: NaN != NaN
		errs = append(errs, "value: must not be NaN")
	}

	// Dimension keys and values must be non-empty if present.
	for k, v := range e.Dimensions {
		if k == "" {
			errs = append(errs, "dimensions: key must not be empty")
		}
		if v == "" {
			errs = append(errs, fmt.Sprintf("dimensions: value for key %q must not be empty", k))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// UnmarshalJSON implements custom JSON unmarshalling to ensure the timestamp
// field is parsed strictly as RFC3339.
func (e *Event) UnmarshalJSON(data []byte) error {
	// Alias to avoid infinite recursion.
	type Alias Event
	aux := &struct {
		Timestamp string `json:"timestamp"`
		*Alias
	}{
		Alias: (*Alias)(e),
	}

	if err := json.Unmarshal(data, aux); err != nil {
		return fmt.Errorf("invalid event JSON: %w", err)
	}

	if aux.Timestamp != "" {
		t, err := time.Parse(time.RFC3339Nano, aux.Timestamp)
		if err != nil {
			return fmt.Errorf("timestamp: must be RFC3339 format, got %q: %w", aux.Timestamp, err)
		}
		e.Timestamp = t.UTC()
	}

	return nil
}
