package window

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/dedup"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/pulse/pulse-engine/internal/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestPipeline() (*PipelineProcessor, *queue.MockProducer, *dedup.MemoryDeduplicator) {
	mgr := config.NewConfigManager()
	cfg := &config.Config{
		Server: config.ServerConfig{
			DLQTopic: "events.dlq",
		},
		Metrics: map[string]config.MetricRule{
			"retail.orders": {
				Window: config.WindowConfig{
					TumblingSizes:      []string{"1m"},
					AllowedLatenessSec: 30,
					DedupTTLSec:        300,
				},
			},
		},
	}
	mgr.Set(cfg)

	memDedup := dedup.NewMemoryDeduplicator()
	dlqMock := queue.NewMockProducer()

	proc := NewPipelineProcessor(mgr, memDedup, nil, dlqMock, nil)
	return proc, dlqMock, memDedup
}

func TestPipelineProcessor_DedupInvariant(t *testing.T) {
	proc, dlqMock, _ := setupTestPipeline()
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 10, 15, 0, 0, 0, time.UTC)

	// Ingest same event 5 times (e.g. client retries)
	for i := 0; i < 5; i++ {
		err := proc.ProcessEvent(ctx, domain.Event{
			EventID:   "evt-dup-check",
			MetricKey: "retail.orders",
			Timestamp: baseTime.Add(10 * time.Second),
			Value:     100.0,
		})
		require.NoError(t, err)
	}

	rollups := proc.aggregators["retail.orders"].FlushAll()
	require.Len(t, rollups, 1)
	assert.Equal(t, uint64(1), rollups[0].EventCount, "Duplicate events must be ignored")
	assert.Equal(t, 100.0, rollups[0].SumValue)
	assert.Len(t, dlqMock.Records["events.dlq"], 0)
}

func TestPipelineProcessor_DLQRouting(t *testing.T) {
	proc, dlqMock, _ := setupTestPipeline()
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 10, 15, 0, 0, 0, time.UTC)

	// Event at 15:10:00 -> sets watermark to 15:09:30 (15:10:00 - 30s)
	err := proc.ProcessEvent(ctx, domain.Event{
		EventID:   "evt-head",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(10 * time.Minute),
		Value:     50.0,
	})
	require.NoError(t, err)

	// Severely late event at 15:02:00 (< watermark 15:09:30) -> should go to DLQ
	err = proc.ProcessEvent(ctx, domain.Event{
		EventID:   "evt-severely-late",
		MetricKey: "retail.orders",
		Timestamp: baseTime.Add(2 * time.Minute),
		Value:     25.0,
	})
	require.NoError(t, err)

	assert.Len(t, dlqMock.Records["events.dlq"], 1)
	assert.Equal(t, "evt-severely-late", dlqMock.Records["events.dlq"][0].EventID)
}

// Property Test: Conservation Law: (Sum of Rollups) + (Sum of DLQ Events) == Total Sum of Unique Ingested Events
func TestPipelineProcessor_ConservationInvariantPropertyTest(t *testing.T) {
	proc, dlqMock, _ := setupTestPipeline()
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 10, 10, 0, 0, 0, time.UTC)

	r := rand.New(rand.NewSource(42))
	var totalUniqueSum float64
	numUniqueEvents := 200

	for i := 0; i < numUniqueEvents; i++ {
		val := float64(r.Intn(100) + 1)
		totalUniqueSum += val

		// Out-of-order event timestamps within first 50 seconds of 10:00
		eventTime := baseTime.Add(time.Duration(r.Intn(50)) * time.Second)
		evtID := fmt.Sprintf("event-%d", i)

		// Ingest event
		err := proc.ProcessEvent(ctx, domain.Event{
			EventID:   evtID,
			MetricKey: "retail.orders",
			Timestamp: eventTime,
			Value:     val,
		})
		require.NoError(t, err)

		// Ingest duplicate 50% of the time (must be deduped)
		if r.Float64() > 0.5 {
			_ = proc.ProcessEvent(ctx, domain.Event{
				EventID:   evtID,
				MetricKey: "retail.orders",
				Timestamp: eventTime,
				Value:     val,
			})
		}
	}

	rollups := proc.aggregators["retail.orders"].FlushAll()
	
	var rollupSum float64
	var rollupCount uint64
	for _, roll := range rollups {
		rollupSum += roll.SumValue
		rollupCount += roll.EventCount
	}

	var dlqSum float64
	var dlqCount uint64
	for _, dlqEvt := range dlqMock.Records["events.dlq"] {
		dlqSum += dlqEvt.Value
		dlqCount++
	}

	// Verify Conservation Law
	assert.Equal(t, uint64(numUniqueEvents), rollupCount+dlqCount, "Total processed (rollups + DLQ) must equal unique incoming events")
	assert.InDelta(t, totalUniqueSum, rollupSum+dlqSum, 0.0001, "Conservation Law: Rollup Sum + DLQ Sum must equal Total Ingested Unique Sum")
}
