package detector

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// EWMAZScoreDetector detects spikes and dips using EWMA smoothing against
// hour-of-week seasonal baselines with z-score thresholds.
type EWMAZScoreDetector struct {
	mu             sync.RWMutex
	baselines      map[string]*SeasonalBaselines // metricKey -> baselines
	ewmaState      map[string]float64            // metricKey -> current EWMA value
	alpha          float64                       // EWMA smoothing factor
	zThreshold     float64                       // |z| threshold for anomaly
	minObservations int                          // Minimum baseline samples before detection
}

func NewEWMAZScoreDetector(alpha, zThreshold float64, minObservations int) *EWMAZScoreDetector {
	if alpha <= 0 || alpha > 1 {
		alpha = 0.25
	}
	if zThreshold <= 0 {
		zThreshold = 3.0
	}
	if minObservations < 1 {
		minObservations = 5
	}
	return &EWMAZScoreDetector{
		baselines:       make(map[string]*SeasonalBaselines),
		ewmaState:       make(map[string]float64),
		alpha:           alpha,
		zThreshold:      zThreshold,
		minObservations: minObservations,
	}
}

func (d *EWMAZScoreDetector) Name() string {
	return "ewma_zscore"
}

func (d *EWMAZScoreDetector) Detect(ctx context.Context, event domain.Event) ([]domain.Anomaly, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := event.MetricKey

	// Get or create baselines for this metric
	sb, exists := d.baselines[key]
	if !exists {
		sb = NewSeasonalBaselines(key)
		d.baselines[key] = sb
	}

	baseline := sb.Get(event.Timestamp)

	// Update EWMA: S_t = alpha * x_t + (1-alpha) * S_{t-1}
	prevEWMA, hasEWMA := d.ewmaState[key]
	var smoothed float64
	if !hasEWMA {
		smoothed = event.Value
	} else {
		smoothed = d.alpha*event.Value + (1-d.alpha)*prevEWMA
	}
	d.ewmaState[key] = smoothed

	var anomalies []domain.Anomaly

	// Only detect if we have enough baseline observations
	if baseline.Count >= int64(d.minObservations) {
		stddev := baseline.StdDev()
		if stddev > 0 {
			zScore := (smoothed - baseline.Mean) / stddev

			if math.Abs(zScore) > d.zThreshold {
				anomalyType := domain.AnomalyTypeSpike
				if zScore < 0 {
					anomalyType = domain.AnomalyTypeDip
				}

				severity := domain.SeverityWarning
				if math.Abs(zScore) > d.zThreshold*1.5 {
					severity = domain.SeverityCritical
				}

				anomalies = append(anomalies, domain.Anomaly{
					ID:             uuid.New().String(),
					EventID:        event.EventID,
					MetricKey:      event.MetricKey,
					Timestamp:      event.Timestamp,
					ObservedValue:  event.Value,
					ExpectedMean:   baseline.Mean,
					ExpectedStdDev: stddev,
					Score:          zScore,
					Type:           anomalyType,
					Severity:       severity,
					Dimensions:     event.Dimensions,
					CreatedAt:      time.Now().UTC(),
				})
			}
		}
	}

	// Always update baseline with current value (after detection, so we detect against historical data)
	sb.Update(event.Value, event.Timestamp)

	return anomalies, nil
}

// GetBaselines returns the baselines for a metric key (used for state persistence and cold-start).
func (d *EWMAZScoreDetector) GetBaselines(metricKey string) *SeasonalBaselines {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.baselines[metricKey]
}

// SetBaselines sets baselines for a metric key (used for cold-start inheritance and state restore).
func (d *EWMAZScoreDetector) SetBaselines(metricKey string, sb *SeasonalBaselines) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.baselines[metricKey] = sb
}
