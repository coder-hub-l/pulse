package ingest

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/pulse/pulse-engine/internal/queue"
)

type HTTPHandler struct {
	cfgManager *config.ConfigManager
	producer   queue.Producer
	logger     *slog.Logger
}

func NewHTTPHandler(cfgManager *config.ConfigManager, producer queue.Producer, logger *slog.Logger) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPHandler{
		cfgManager: cfgManager,
		producer:   producer,
		logger:     logger,
	}
}

type IngestResponse struct {
	Success  bool     `json:"success"`
	Ingested int      `json:"ingested"`
	Errors   []string `json:"errors,omitempty"`
}

// HandleIngestEvent handles single JSON event ingestion: POST /v1/events
func (h *HTTPHandler) HandleIngestEvent(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20)) // 1MB limit
	if err != nil {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Failed to read request body"}})
		return
	}
	defer r.Body.Close()

	var event domain.Event
	if err := json.Unmarshal(body, &event); err != nil {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Invalid JSON: " + err.Error()}})
		return
	}

	cfg := h.cfgManager.Get()
	if cfg == nil {
		h.respondJSON(w, http.StatusInternalServerError, IngestResponse{Success: false, Errors: []string{"Server configuration unavailable"}})
		return
	}

	// Verify metric is registered
	if _, exists := cfg.Metrics[event.MetricKey]; !exists {
		h.respondJSON(w, http.StatusNotFound, IngestResponse{Success: false, Errors: []string{"Unknown metric_key: " + event.MetricKey}})
		return
	}

	now := time.Now().UTC()
	futSkew := time.Duration(cfg.Server.IngestMaxFutureSkewSec) * time.Second
	pastSkew := time.Duration(cfg.Server.IngestMaxPastSkewSec) * time.Second

	if err := event.Validate(now, futSkew, pastSkew); err != nil {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Validation error: " + err.Error()}})
		return
	}

	if err := h.producer.Publish(r.Context(), cfg.Server.RawEventsTopic, event); err != nil {
		h.logger.Error("Failed to publish event to Redpanda", "metric_key", event.MetricKey, "err", err)
		h.respondJSON(w, http.StatusServiceUnavailable, IngestResponse{Success: false, Errors: []string{"Buffer unavailable"}})
		return
	}

	h.respondJSON(w, http.StatusAccepted, IngestResponse{Success: true, Ingested: 1})
}

// HandleIngestBatch handles batch JSON events: POST /v1/events/batch
func (h *HTTPHandler) HandleIngestBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20)) // 10MB limit
	if err != nil {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Failed to read request body"}})
		return
	}
	defer r.Body.Close()

	var batch domain.Batch
	if err := json.Unmarshal(body, &batch); err != nil {
		// Attempt array fallback
		var events []domain.Event
		if errArray := json.Unmarshal(body, &events); errArray != nil {
			h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Invalid JSON array or batch object"}})
			return
		}
		batch.Events = events
	}

	if len(batch.Events) == 0 {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Errors: []string{"Batch contains no events"}})
		return
	}

	cfg := h.cfgManager.Get()
	if cfg == nil {
		h.respondJSON(w, http.StatusInternalServerError, IngestResponse{Success: false, Errors: []string{"Server configuration unavailable"}})
		return
	}

	now := time.Now().UTC()
	futSkew := time.Duration(cfg.Server.IngestMaxFutureSkewSec) * time.Second
	pastSkew := time.Duration(cfg.Server.IngestMaxPastSkewSec) * time.Second

	validEvents := make([]domain.Event, 0, len(batch.Events))
	var valErrors []string

	for idx, evt := range batch.Events {
		if _, exists := cfg.Metrics[evt.MetricKey]; !exists {
			valErrors = append(valErrors, "event["+string(rune('0'+idx))+"]: unknown metric_key "+evt.MetricKey)
			continue
		}
		if err := evt.Validate(now, futSkew, pastSkew); err != nil {
			valErrors = append(valErrors, "event["+string(rune('0'+idx))+"]: "+err.Error())
			continue
		}
		validEvents = append(validEvents, evt)
	}

	if len(validEvents) == 0 {
		h.respondJSON(w, http.StatusBadRequest, IngestResponse{Success: false, Ingested: 0, Errors: valErrors})
		return
	}

	published, err := h.producer.PublishBatch(r.Context(), cfg.Server.RawEventsTopic, validEvents)
	if err != nil {
		h.logger.Error("Partial/Failed batch publish to Redpanda", "err", err, "published", published)
		valErrors = append(valErrors, "Failed publishing all events: "+err.Error())
	}

	status := http.StatusAccepted
	if len(valErrors) > 0 && published > 0 {
		status = http.StatusMultiStatus
	}

	h.respondJSON(w, status, IngestResponse{
		Success:  published > 0,
		Ingested: published,
		Errors:   valErrors,
	})
}

// HandleHealthz handles liveness/readiness probes: GET /healthz
func (h *HTTPHandler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"ok"}`))
}

func (h *HTTPHandler) respondJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
