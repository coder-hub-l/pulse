package model

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

func validEvent() Event {
	return Event{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		MetricKey: "api.latency_p99",
		Timestamp: time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC),
		Value:     42.5,
		Dimensions: map[string]string{
			"region": "us-east-1",
			"env":    "prod",
		},
	}
}

func TestValidateHappyPath(t *testing.T) {
	e := validEvent()
	if err := e.Validate(); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestValidateZeroValueIsValid(t *testing.T) {
	e := validEvent()
	e.Value = 0.0
	if err := e.Validate(); err != nil {
		t.Fatalf("value=0 should be valid, got: %v", err)
	}
}

func TestValidateNegativeValueIsValid(t *testing.T) {
	e := validEvent()
	e.Value = -99.9
	if err := e.Validate(); err != nil {
		t.Fatalf("negative value should be valid, got: %v", err)
	}
}

func TestValidateEmptyEventID(t *testing.T) {
	e := validEvent()
	e.EventID = ""
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for empty event_id")
	}
	if !strings.Contains(err.Error(), "event_id") {
		t.Fatalf("error should mention event_id, got: %v", err)
	}
}

func TestValidateNonUUIDEventID(t *testing.T) {
	e := validEvent()
	e.EventID = "not-a-uuid"
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for non-UUID event_id")
	}
	if !strings.Contains(err.Error(), "event_id") {
		t.Fatalf("error should mention event_id, got: %v", err)
	}
}

func TestValidateEmptyMetricKey(t *testing.T) {
	e := validEvent()
	e.MetricKey = ""
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for empty metric_key")
	}
	if !strings.Contains(err.Error(), "metric_key") {
		t.Fatalf("error should mention metric_key, got: %v", err)
	}
}

func TestValidateMetricKeyTooLong(t *testing.T) {
	e := validEvent()
	e.MetricKey = strings.Repeat("a", 257)
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for metric_key > 256 chars")
	}
	if !strings.Contains(err.Error(), "metric_key") {
		t.Fatalf("error should mention metric_key, got: %v", err)
	}
}

func TestValidateZeroTimestamp(t *testing.T) {
	e := validEvent()
	e.Timestamp = time.Time{}
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for zero timestamp")
	}
	if !strings.Contains(err.Error(), "timestamp") {
		t.Fatalf("error should mention timestamp, got: %v", err)
	}
}

func TestValidateNaNValue(t *testing.T) {
	e := validEvent()
	e.Value = math.NaN()
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for NaN value")
	}
	if !strings.Contains(err.Error(), "NaN") {
		t.Fatalf("error should mention NaN, got: %v", err)
	}
}

func TestValidateEmptyDimensionKey(t *testing.T) {
	e := validEvent()
	e.Dimensions[""] = "oops"
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for empty dimension key")
	}
	if !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("error should mention dimensions, got: %v", err)
	}
}

func TestValidateEmptyDimensionValue(t *testing.T) {
	e := validEvent()
	e.Dimensions["host"] = ""
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error for empty dimension value")
	}
	if !strings.Contains(err.Error(), "dimensions") {
		t.Fatalf("error should mention dimensions, got: %v", err)
	}
}

func TestValidateMultipleErrors(t *testing.T) {
	e := Event{} // Everything wrong
	err := e.Validate()
	if err == nil {
		t.Fatal("expected errors")
	}
	msg := err.Error()
	// Should report event_id, metric_key, and timestamp errors all at once.
	for _, field := range []string{"event_id", "metric_key", "timestamp"} {
		if !strings.Contains(msg, field) {
			t.Errorf("combined error should mention %q, got: %s", field, msg)
		}
	}
}

func TestValidateNoDimensions(t *testing.T) {
	e := validEvent()
	e.Dimensions = nil
	if err := e.Validate(); err != nil {
		t.Fatalf("nil dimensions should be valid, got: %v", err)
	}
}

// --- DimensionKey tests ---

func TestDimensionKeyNoDimensions(t *testing.T) {
	e := validEvent()
	e.Dimensions = nil
	if got := e.DimensionKey(); got != "api.latency_p99" {
		t.Fatalf("expected bare metric_key, got %q", got)
	}
}

func TestDimensionKeySorted(t *testing.T) {
	e := validEvent()
	// Dimensions: env=prod, region=us-east-1 → sorted by key.
	want := "api.latency_p99|env=prod|region=us-east-1"
	if got := e.DimensionKey(); got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestDimensionKeyDeterministic(t *testing.T) {
	e := validEvent()
	first := e.DimensionKey()
	for i := 0; i < 100; i++ {
		if got := e.DimensionKey(); got != first {
			t.Fatalf("DimensionKey not deterministic: %q vs %q", first, got)
		}
	}
}

// --- JSON unmarshalling tests ---

func TestUnmarshalValidJSON(t *testing.T) {
	raw := `{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"metric_key": "cpu.usage",
		"timestamp": "2025-06-15T10:30:00Z",
		"value": 75.3,
		"dimensions": {"host": "web-01"}
	}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if e.MetricKey != "cpu.usage" {
		t.Errorf("metric_key: got %q", e.MetricKey)
	}
	if e.Value != 75.3 {
		t.Errorf("value: got %f", e.Value)
	}
	if e.Timestamp.IsZero() {
		t.Error("timestamp should not be zero")
	}
	if e.Timestamp.Location() != time.UTC {
		t.Error("timestamp should be normalized to UTC")
	}
}

func TestUnmarshalBadTimestampFormat(t *testing.T) {
	raw := `{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"metric_key": "cpu.usage",
		"timestamp": "June 15, 2025",
		"value": 1.0
	}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err == nil {
		t.Fatal("expected error for non-RFC3339 timestamp")
	}
}

func TestUnmarshalNanoSecondTimestamp(t *testing.T) {
	raw := `{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"metric_key": "cpu.usage",
		"timestamp": "2025-06-15T10:30:00.123456789Z",
		"value": 1.0
	}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if e.Timestamp.Nanosecond() != 123456789 {
		t.Errorf("expected nanoseconds preserved, got %d", e.Timestamp.Nanosecond())
	}
}

func TestUnmarshalTimestampNonUTC(t *testing.T) {
	// Timestamp with +05:30 offset should be normalized to UTC.
	raw := `{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"metric_key": "cpu.usage",
		"timestamp": "2025-06-15T16:00:00+05:30",
		"value": 1.0
	}`
	var e Event
	if err := json.Unmarshal([]byte(raw), &e); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if e.Timestamp.Location() != time.UTC {
		t.Errorf("timestamp should be UTC, got %v", e.Timestamp.Location())
	}
	// 16:00 +05:30 = 10:30 UTC
	if e.Timestamp.Hour() != 10 || e.Timestamp.Minute() != 30 {
		t.Errorf("expected 10:30 UTC, got %02d:%02d", e.Timestamp.Hour(), e.Timestamp.Minute())
	}
}
