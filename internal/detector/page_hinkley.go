package detector

import (
	"context"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// PageHinkleyDetector implements the Page-Hinkley test for gradual drift detection.
// Unlike spike detectors, it accumulates small consistent shifts over time.
//
// Algorithm:
//   m_t = m_{t-1} + (x_t - mean - delta)     (cumulative sum of deviations)
//   M_t = min(M_{t-1}, m_t)                   (running minimum)
//   PH_t = m_t - M_t                           (test statistic)
//   Drift detected when PH_t > threshold
type PageHinkleyDetector struct {
	mu        sync.Mutex
	states    map[string]*phState
	delta     float64 // Minimum magnitude of detectable change
	threshold float64 // Decision threshold for cumulative sum
	alpha     float64 // Forgetting factor for running mean (0 < alpha < 1)
}

type phState struct {
	Count   int64
	Mean    float64 // Running mean with exponential forgetting
	CumSum  float64 // Cumulative sum m_t
	MinSum  float64 // Running minimum M_t
}

func NewPageHinkleyDetector(delta, threshold, alpha float64) *PageHinkleyDetector {
	if delta <= 0 {
		delta = 0.1
	}
	if threshold <= 0 {
		threshold = 15.0
	}
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.99
	}
	return &PageHinkleyDetector{
		states:    make(map[string]*phState),
		delta:     delta,
		threshold: threshold,
		alpha:     alpha,
	}
}

func (d *PageHinkleyDetector) Name() string {
	return "page_hinkley"
}

func (d *PageHinkleyDetector) Detect(ctx context.Context, event domain.Event) ([]domain.Anomaly, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	key := event.MetricKey
	st, exists := d.states[key]
	if !exists {
		st = &phState{
			Mean:   event.Value,
			Count:  1,
			CumSum: 0,
			MinSum: 0,
		}
		d.states[key] = st
		return nil, nil
	}

	st.Count++

	// Update exponentially weighted running mean
	st.Mean = d.alpha*st.Mean + (1-d.alpha)*event.Value

	// Accumulate deviation from mean minus delta
	st.CumSum += event.Value - st.Mean - d.delta

	// Track running minimum
	if st.CumSum < st.MinSum {
		st.MinSum = st.CumSum
	}

	// Page-Hinkley test statistic
	phStat := st.CumSum - st.MinSum

	var anomalies []domain.Anomaly

	if phStat > d.threshold && st.Count > 30 {
		anomalies = append(anomalies, domain.Anomaly{
			ID:             uuid.New().String(),
			EventID:        event.EventID,
			MetricKey:      event.MetricKey,
			Timestamp:      event.Timestamp,
			ObservedValue:  event.Value,
			ExpectedMean:   st.Mean,
			ExpectedStdDev: 0, // Drift detector doesn't use stddev
			Score:          phStat,
			Type:           domain.AnomalyTypeDrift,
			Severity:       d.classifySeverity(phStat),
			Dimensions:     event.Dimensions,
			CreatedAt:      time.Now().UTC(),
		})

		// Reset state after detecting drift (start fresh from new regime)
		st.CumSum = 0
		st.MinSum = 0
		st.Mean = event.Value
		st.Count = 1
	}

	return anomalies, nil
}

func (d *PageHinkleyDetector) classifySeverity(stat float64) domain.Severity {
	ratio := stat / d.threshold
	if ratio > 3.0 {
		return domain.SeverityCritical
	}
	if ratio > 1.5 {
		return domain.SeverityWarning
	}
	return domain.SeverityInfo
}

// Reset clears state for a metric key.
func (d *PageHinkleyDetector) Reset(metricKey string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.states, metricKey)
}

// GetStatistic returns the current PH test statistic for a key (for observability/testing).
func (d *PageHinkleyDetector) GetStatistic(metricKey string) float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	st, exists := d.states[metricKey]
	if !exists {
		return 0
	}
	return math.Max(0, st.CumSum-st.MinSum)
}
