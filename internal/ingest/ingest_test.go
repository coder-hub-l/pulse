package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/pulse/pulse-engine/internal/queue"
	pulsev1 "github.com/pulse/pulse-engine/pkg/proto/pulse/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestManager() *config.ConfigManager {
	mgr := config.NewConfigManager()
	cfg := &config.Config{
		Server: config.ServerConfig{
			RawEventsTopic:         "events.raw",
			IngestMaxFutureSkewSec: 60,
			IngestMaxPastSkewSec:   3600,
		},
		Metrics: map[string]config.MetricRule{
			"retail.store.orders": {},
			"api.latency.p99":     {},
		},
	}
	mgr.Set(cfg)
	return mgr
}

func TestHTTPHandler_HandleIngestEvent_Success(t *testing.T) {
	mgr := setupTestManager()
	mockProducer := queue.NewMockProducer()
	handler := NewHTTPHandler(mgr, mockProducer, nil)

	event := domain.Event{
		EventID:   "evt-001",
		MetricKey: "retail.store.orders",
		Timestamp: time.Now().UTC(),
		Value:     42.0,
		Dimensions: map[string]string{
			"store_id": "store_101",
		},
	}

	payload, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(payload))
	rr := httptest.NewRecorder()

	handler.HandleIngestEvent(rr, req)

	assert.Equal(t, http.StatusAccepted, rr.Code)
	assert.Len(t, mockProducer.Records["events.raw"], 1)
	assert.Equal(t, "evt-001", mockProducer.Records["events.raw"][0].EventID)
}

func TestHTTPHandler_HandleIngestEvent_UnknownMetric(t *testing.T) {
	mgr := setupTestManager()
	mockProducer := queue.NewMockProducer()
	handler := NewHTTPHandler(mgr, mockProducer, nil)

	event := domain.Event{
		EventID:   "evt-002",
		MetricKey: "unknown.metric.key",
		Timestamp: time.Now().UTC(),
		Value:     10.0,
	}

	payload, _ := json.Marshal(event)
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewReader(payload))
	rr := httptest.NewRecorder()

	handler.HandleIngestEvent(rr, req)

	assert.Equal(t, http.StatusNotFound, rr.Code)
	assert.Len(t, mockProducer.Records["events.raw"], 0)
}

func TestHTTPHandler_HandleIngestBatch(t *testing.T) {
	mgr := setupTestManager()
	mockProducer := queue.NewMockProducer()
	handler := NewHTTPHandler(mgr, mockProducer, nil)

	now := time.Now().UTC()
	batch := domain.Batch{
		Events: []domain.Event{
			{
				EventID:   "evt-b1",
				MetricKey: "retail.store.orders",
				Timestamp: now,
				Value:     1.0,
			},
			{
				EventID:   "evt-b2",
				MetricKey: "api.latency.p99",
				Timestamp: now,
				Value:     150.0,
			},
			{
				EventID:   "evt-b3",
				MetricKey: "unknown.series",
				Timestamp: now,
				Value:     99.0,
			},
		},
	}

	payload, _ := json.Marshal(batch)
	req := httptest.NewRequest(http.MethodPost, "/v1/events/batch", bytes.NewReader(payload))
	rr := httptest.NewRecorder()

	handler.HandleIngestBatch(rr, req)

	assert.Equal(t, http.StatusMultiStatus, rr.Code)
	assert.Len(t, mockProducer.Records["events.raw"], 2)
}

func TestGRPCHandler_IngestEvents(t *testing.T) {
	mgr := setupTestManager()
	mockProducer := queue.NewMockProducer()
	grpcHandler := NewGRPCHandler(mgr, mockProducer)

	req := &pulsev1.IngestRequest{
		Events: []*pulsev1.EventProto{
			{
				EventId:         "grpc-1",
				MetricKey:       "retail.store.orders",
				TimestampUnixMs: time.Now().UnixMilli(),
				Value:           12.5,
				Dimensions:      map[string]string{"region": "us-east"},
			},
			{
				EventId:         "grpc-2",
				MetricKey:       "invalid.metric",
				TimestampUnixMs: time.Now().UnixMilli(),
				Value:           5.0,
			},
		},
	}

	resp, err := grpcHandler.IngestEvents(context.Background(), req)
	require.NoError(t, err)
	assert.Equal(t, uint32(1), resp.AcceptedCount)
	assert.Equal(t, uint32(1), resp.RejectedCount)
	assert.Len(t, mockProducer.Records["events.raw"], 1)
}
