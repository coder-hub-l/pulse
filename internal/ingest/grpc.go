package ingest

import (
	"context"
	"fmt"
	"time"

	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/pulse/pulse-engine/internal/queue"
	pulsev1 "github.com/pulse/pulse-engine/pkg/proto/pulse/v1"
)

type GRPCHandler struct {
	cfgManager *config.ConfigManager
	producer   queue.Producer
}

func NewGRPCHandler(cfgManager *config.ConfigManager, producer queue.Producer) *GRPCHandler {
	return &GRPCHandler{
		cfgManager: cfgManager,
		producer:   producer,
	}
}

func (g *GRPCHandler) IngestEvents(ctx context.Context, req *pulsev1.IngestRequest) (*pulsev1.IngestResponse, error) {
	if req == nil || len(req.Events) == 0 {
		return &pulsev1.IngestResponse{
			AcceptedCount: 0,
			RejectedCount: 0,
			Errors:        []string{"No events provided"},
		}, nil
	}

	cfg := g.cfgManager.Get()
	if cfg == nil {
		return nil, fmt.Errorf("server configuration unavailable")
	}

	now := time.Now().UTC()
	futSkew := time.Duration(cfg.Server.IngestMaxFutureSkewSec) * time.Second
	pastSkew := time.Duration(cfg.Server.IngestMaxPastSkewSec) * time.Second

	validEvents := make([]domain.Event, 0, len(req.Events))
	var errMessages []string

	for idx, pEvt := range req.Events {
		dEvt := pEvt.ToDomain()
		if _, exists := cfg.Metrics[dEvt.MetricKey]; !exists {
			errMessages = append(errMessages, fmt.Sprintf("event[%d]: unknown metric_key %s", idx, dEvt.MetricKey))
			continue
		}
		if err := dEvt.Validate(now, futSkew, pastSkew); err != nil {
			errMessages = append(errMessages, fmt.Sprintf("event[%d]: %s", idx, err.Error()))
			continue
		}
		validEvents = append(validEvents, dEvt)
	}

	if len(validEvents) > 0 {
		_, err := g.producer.PublishBatch(ctx, cfg.Server.RawEventsTopic, validEvents)
		if err != nil {
			errMessages = append(errMessages, "Failed to publish to buffer: "+err.Error())
		}
	}

	return &pulsev1.IngestResponse{
		AcceptedCount: uint32(len(validEvents)),
		RejectedCount: uint32(len(req.Events) - len(validEvents)),
		Errors:        errMessages,
	}, nil
}
