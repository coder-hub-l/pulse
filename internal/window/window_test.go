package window

import (
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWindowAggregator_BasicAggregation(t *testing.T) {
	baseTime := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	resolutions := []time.Duration{time.Minute, 5 * time.Minute}
	names := []string{"1m", "5m"}
	allowedLateness := 30 * time.Second

	agg := NewWindowAggregator("retail.orders", resolutions, names, allowedLateness)

	// Ingest 3 events in the same 1m window [12:00:00, 12:01:00)
	agg.Ingest(domain.Event{
		EventID:   "e1",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(10 * time.Second),
		Value:     10.0,
	})
	agg.Ingest(domain.Event{
		EventID:   "e2",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(20 * time.Second),
		Value:     20.0,
	})
	agg.Ingest(domain.Event{
		EventID:   "e3",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(30 * time.Second),
		Value:     30.0,
	})

	rollups := agg.FlushAll()
	require.Len(t, rollups, 2) // 1m rollup and 5m rollup

	var r1m *domain.Rollup
	for i := range rollups {
		if rollups[i].WindowResolution == "1m" {
			r1m = &rollups[i]
		}
	}

	require.NotNil(t, r1m)
	assert.Equal(t, uint64(3), r1m.EventCount)
	assert.Equal(t, 60.0, r1m.SumValue)
	assert.Equal(t, 20.0, r1m.MeanValue)
	assert.Equal(t, 10.0, r1m.MinValue)
	assert.Equal(t, 30.0, r1m.MaxValue)
	assert.Equal(t, 20.0, r1m.P50Value)
}

func TestWindowAggregator_LateEvents_DLQ(t *testing.T) {
	baseTime := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	resolutions := []time.Duration{time.Minute}
	names := []string{"1m"}
	allowedLateness := 10 * time.Second

	agg := NewWindowAggregator("retail.orders", resolutions, names, allowedLateness)

	// Event at 12:05:00 advances max observed to 12:05:00. Watermark = 12:05:00 - 10s = 12:04:50.
	_, isDLQ := agg.Ingest(domain.Event{
		EventID:   "e-future",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(5 * time.Minute),
		Value:     100.0,
	})
	assert.False(t, isDLQ)

	// Event at 12:04:55 arrives (within lateness: after 12:04:50) -> Accepted!
	_, isDLQ = agg.Ingest(domain.Event{
		EventID:   "e-within-lateness",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(4*time.Minute + 55*time.Second),
		Value:     50.0,
	})
	assert.False(t, isDLQ)

	// Event at 12:01:00 arrives (older than watermark 12:04:50) -> DLQ!
	_, isDLQ = agg.Ingest(domain.Event{
		EventID:   "e-expired",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(1 * time.Minute),
		Value:     50.0,
	})
	assert.True(t, isDLQ, "Event older than watermark must be routed to DLQ")
}

func TestWindowAggregator_WatermarkEviction(t *testing.T) {
	baseTime := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	resolutions := []time.Duration{time.Minute}
	names := []string{"1m"}
	allowedLateness := 10 * time.Second

	agg := NewWindowAggregator("retail.orders", resolutions, names, allowedLateness)

	// Event in window [12:00:00, 12:01:00)
	agg.Ingest(domain.Event{
		EventID:   "e1",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(15 * time.Second),
		Value:     10.0,
	})

	// Event at 12:01:15 advances watermark to 12:01:05 (which is >= 12:01:00)
	closed, _ := agg.Ingest(domain.Event{
		EventID:   "e2",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(1*time.Minute + 15*time.Second),
		Value:     20.0,
	})

	require.Len(t, closed, 1, "Window [12:00, 12:01) should automatically seal when watermark passes 12:01:00")
	assert.Equal(t, "1m", closed[0].WindowResolution)
	assert.Equal(t, uint64(1), closed[0].EventCount)
	assert.Equal(t, 10.0, closed[0].SumValue)
}
