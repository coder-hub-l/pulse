package window

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/pulse/pulse-engine/internal/alert"
	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/dedup"
	"github.com/pulse/pulse-engine/internal/detector"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/pulse/pulse-engine/internal/observability"
	"github.com/pulse/pulse-engine/internal/queue"
	"github.com/pulse/pulse-engine/internal/storage"
)

// PipelineProcessor coordinates dedup, window aggregation, anomaly detection, alerting, and storage flushing.
type PipelineProcessor struct {
	cfgManager    *config.ConfigManager
	dedup         dedup.Deduplicator
	clickhouse    *storage.ClickHouseClient
	dlqProducer   queue.Producer
	aggregators   map[string]*WindowAggregator
	spikeDetectors map[string]*detector.EWMAZScoreDetector
	driftDetectors map[string]*detector.PageHinkleyDetector
	silenceDetector *detector.SilenceDetector
	alertEngine   *alert.Engine
	mu            sync.RWMutex
	logger        *slog.Logger
}

func NewPipelineProcessor(
	cfgManager *config.ConfigManager,
	dedup dedup.Deduplicator,
	clickhouse *storage.ClickHouseClient,
	dlqProducer queue.Producer,
	logger *slog.Logger,
) *PipelineProcessor {
	if logger == nil {
		logger = slog.Default()
	}
	return &PipelineProcessor{
		cfgManager:     cfgManager,
		dedup:          dedup,
		clickhouse:     clickhouse,
		dlqProducer:    dlqProducer,
		aggregators:    make(map[string]*WindowAggregator),
		spikeDetectors: make(map[string]*detector.EWMAZScoreDetector),
		driftDetectors: make(map[string]*detector.PageHinkleyDetector),
		silenceDetector: detector.NewSilenceDetector(2 * time.Minute),
		logger:         logger,
	}
}

// SetAlertEngine sets the alert engine used to dispatch anomalies to sinks.
func (p *PipelineProcessor) SetAlertEngine(ae *alert.Engine) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.alertEngine = ae
}

// ProcessEvent runs the streaming pipeline for a single event:
// 1. Dedup check via Redis
// 2. Fetch or create WindowAggregator
// 3. Ingest into window buckets (with watermark updates)
// 4. If expired past allowed lateness -> publish to DLQ
// 5. If closed windows produced -> flush to ClickHouse
func (p *PipelineProcessor) ProcessEvent(ctx context.Context, event domain.Event) error {
	cfg := p.cfgManager.Get()
	if cfg == nil {
		return fmt.Errorf("active config is nil")
	}

	rule, exists := cfg.Metrics[event.MetricKey]
	if !exists {
		p.logger.Warn("Dropping event for unconfigured metric", "metric_key", event.MetricKey)
		return nil
	}

	// 1. Deduplication
	dedupTTL := rule.DedupTTL()
	isDup, err := p.dedup.IsDuplicate(ctx, event.MetricKey, event.EventID, dedupTTL)
	if err != nil {
		p.logger.Error("Dedup check error, proceeding to avoid data loss", "err", err)
	} else if isDup {
		p.logger.Debug("Duplicate event dropped", "event_id", event.EventID, "metric_key", event.MetricKey)
		return nil
	}

	// 2. Aggregator retrieval
	agg := p.getOrCreateAggregator(event.MetricKey, rule)

	// 3. Ingest & Window computation
	closedRollups, isDLQ := agg.Ingest(event)

	// 4. DLQ Routing for severely late events
	if isDLQ {
		p.logger.Warn("Event exceeded allowed lateness, routing to DLQ", "event_id", event.EventID, "metric_key", event.MetricKey, "ts", event.Timestamp)
		if p.dlqProducer != nil {
			_ = p.dlqProducer.Publish(ctx, cfg.Server.DLQTopic, event)
		}
	}

	// 5. Columnar Batch Write of closed rollups to ClickHouse
	if len(closedRollups) > 0 && p.clickhouse != nil {
		if err := p.clickhouse.WriteRollupsBatch(ctx, closedRollups); err != nil {
			p.logger.Error("Failed to write rollups to ClickHouse", "count", len(closedRollups), "err", err)
		}
	}

	// 6. Anomaly Detection
	var detectedAnomalies []domain.Anomaly

	// 6a. Spike / Dip Detection (EWMA + Z-Score)
	if rule.Detectors.Spike.Enabled {
		spikeDet := p.getOrCreateSpikeDetector(event.MetricKey, rule.Detectors.Spike, rule.Baselines.MinObservations)
		if anoms, err := spikeDet.Detect(ctx, event); err == nil && len(anoms) > 0 {
			detectedAnomalies = append(detectedAnomalies, anoms...)
		}
	}

	// 6b. Gradual Drift Detection (Page-Hinkley)
	if rule.Detectors.Drift.Enabled {
		driftDet := p.getOrCreateDriftDetector(event.MetricKey, rule.Detectors.Drift)
		if anoms, err := driftDet.Detect(ctx, event); err == nil && len(anoms) > 0 {
			detectedAnomalies = append(detectedAnomalies, anoms...)
		}
	}

	// 6c. Silence / Heartbeat tracking
	if rule.Detectors.Silence.Enabled {
		p.silenceDetector.RecordActivity(event.MetricKey, time.Now().UTC())
	}

	// 7. Anomaly Persistence & Metrics
	if len(detectedAnomalies) > 0 {
		for _, a := range detectedAnomalies {
			observability.AnomaliesDetectedTotal.WithLabelValues(a.MetricKey, string(a.Type), string(a.Severity)).Inc()
		}

		if p.clickhouse != nil {
			if err := p.clickhouse.WriteAnomaliesBatch(ctx, detectedAnomalies); err != nil {
				p.logger.Error("Failed to write anomalies to ClickHouse", "count", len(detectedAnomalies), "err", err)
			}
		}

		// 8. Alert Engine: Fingerprinting, Cooldown, and Sink Dispatch
		p.mu.RLock()
		ae := p.alertEngine
		p.mu.RUnlock()

		if ae != nil {
			firedAlerts := ae.Process(detectedAnomalies)
			for _, alert := range firedAlerts {
				observability.AlertsFiredTotal.WithLabelValues(alert.MetricKey, string(alert.Severity)).Inc()
				p.logger.Info("Alert fired", "id", alert.ID, "title", alert.Title, "severity", alert.Severity)
			}
		}
	}

	return nil
}

func (p *PipelineProcessor) getOrCreateSpikeDetector(metricKey string, cfg config.SpikeDetectorConfig, minObs int) *detector.EWMAZScoreDetector {
	p.mu.RLock()
	det, exists := p.spikeDetectors[metricKey]
	p.mu.RUnlock()
	if exists {
		return det
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if det, exists := p.spikeDetectors[metricKey]; exists {
		return det
	}
	det = detector.NewEWMAZScoreDetector(cfg.Alpha, cfg.ZThreshold, minObs)
	p.spikeDetectors[metricKey] = det
	return det
}

func (p *PipelineProcessor) getOrCreateDriftDetector(metricKey string, cfg config.DriftDetectorConfig) *detector.PageHinkleyDetector {
	p.mu.RLock()
	det, exists := p.driftDetectors[metricKey]
	p.mu.RUnlock()
	if exists {
		return det
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if det, exists := p.driftDetectors[metricKey]; exists {
		return det
	}
	det = detector.NewPageHinkleyDetector(cfg.Delta, cfg.Threshold, cfg.Alpha)
	p.driftDetectors[metricKey] = det
	return det
}

func (p *PipelineProcessor) getOrCreateAggregator(metricKey string, rule config.MetricRule) *WindowAggregator {
	p.mu.RLock()
	agg, exists := p.aggregators[metricKey]
	p.mu.RUnlock()

	if exists {
		return agg
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if agg, exists := p.aggregators[metricKey]; exists {
		return agg
	}

	var resDurs []time.Duration
	var resNames []string

	for _, size := range rule.Window.TumblingSizes {
		dur, err := config.ParseDuration(size)
		if err == nil {
			resDurs = append(resDurs, dur)
			resNames = append(resNames, size)
		}
	}

	if len(resDurs) == 0 {
		resDurs = []time.Duration{time.Minute}
		resNames = []string{"1m"}
	}

	agg = NewWindowAggregator(metricKey, resDurs, resNames, rule.AllowedLateness())
	p.aggregators[metricKey] = agg
	return agg
}

// FlushAll seals and flushes all pending window buckets across all aggregators.
func (p *PipelineProcessor) FlushAll(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var allRollups []domain.Rollup
	for _, agg := range p.aggregators {
		allRollups = append(allRollups, agg.FlushAll()...)
	}

	if len(allRollups) > 0 && p.clickhouse != nil {
		return p.clickhouse.WriteRollupsBatch(ctx, allRollups)
	}
	return nil
}
