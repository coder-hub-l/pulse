package detector

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// SilenceDetector monitors heartbeat liveness per metric key.
// If no events arrive within the configured timeout, it fires a silence anomaly.
// "No data" is fundamentally different from "value = 0".
type SilenceDetector struct {
	mu       sync.Mutex
	lastSeen map[string]time.Time // metricKey -> last event timestamp (wall clock)
	timeout  time.Duration
	fired    map[string]bool // Track whether we already fired for current silence window
}

func NewSilenceDetector(timeout time.Duration) *SilenceDetector {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	return &SilenceDetector{
		lastSeen: make(map[string]time.Time),
		timeout:  timeout,
		fired:    make(map[string]bool),
	}
}

func (d *SilenceDetector) Name() string {
	return "silence"
}

// RecordActivity marks that a metric key has produced data at wall-clock time now.
// Called on every successfully ingested event to reset the heartbeat timer.
func (d *SilenceDetector) RecordActivity(metricKey string, now time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lastSeen[metricKey] = now
	d.fired[metricKey] = false // Reset fired flag when data resumes
}

// Detect is a no-op for silence detector (silence is detected by CheckAll, not per-event).
func (d *SilenceDetector) Detect(ctx context.Context, event domain.Event) ([]domain.Anomaly, error) {
	d.RecordActivity(event.MetricKey, time.Now().UTC())
	return nil, nil
}

// CheckAll scans all tracked metric keys and returns silence anomalies for any
// keys that have not produced data within the timeout window.
// This should be called periodically by a background ticker goroutine.
func (d *SilenceDetector) CheckAll(now time.Time) []domain.Anomaly {
	d.mu.Lock()
	defer d.mu.Unlock()

	var anomalies []domain.Anomaly

	for metricKey, lastTime := range d.lastSeen {
		if d.fired[metricKey] {
			continue // Already fired for this silence period
		}

		elapsed := now.Sub(lastTime)
		if elapsed > d.timeout {
			anomalies = append(anomalies, domain.Anomaly{
				ID:             uuid.New().String(),
				EventID:        "",
				MetricKey:      metricKey,
				Timestamp:      now,
				ObservedValue:  0,
				ExpectedMean:   0,
				ExpectedStdDev: 0,
				Score:          elapsed.Seconds(),
				Type:           domain.AnomalyTypeSilence,
				Severity:       d.classifySeverity(elapsed),
				CreatedAt:      now,
			})
			d.fired[metricKey] = true
		}
	}

	return anomalies
}

// Remove stops tracking a metric key (e.g., when it is decommissioned).
func (d *SilenceDetector) Remove(metricKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.lastSeen, metricKey)
	delete(d.fired, metricKey)
}

// TrackedKeys returns the set of metric keys currently monitored for heartbeats.
func (d *SilenceDetector) TrackedKeys() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	keys := make([]string, 0, len(d.lastSeen))
	for k := range d.lastSeen {
		keys = append(keys, k)
	}
	return keys
}

func (d *SilenceDetector) classifySeverity(elapsed time.Duration) domain.Severity {
	ratio := float64(elapsed) / float64(d.timeout)
	if ratio > 5.0 {
		return domain.SeverityCritical
	}
	if ratio > 2.0 {
		return domain.SeverityWarning
	}
	return domain.SeverityInfo
}
