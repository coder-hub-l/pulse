package detector

import (
	"context"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
)

// Detector is the interface all anomaly detection algorithms must implement.
type Detector interface {
	// Detect evaluates a single metric observation and returns any anomalies found.
	Detect(ctx context.Context, event domain.Event) ([]domain.Anomaly, error)

	// Name returns the detector algorithm identifier.
	Name() string
}

// Baseline holds the online statistical summary for a single hour-of-week slot.
// Uses Welford's online algorithm for numerically stable incremental mean/variance.
type Baseline struct {
	Count    int64   `json:"count"`
	Mean     float64 `json:"mean"`
	M2       float64 `json:"m2"` // Sum of squared deviations from the mean
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	LastSeen time.Time `json:"last_seen"`
}

// Update applies a new observation using Welford's online algorithm.
func (b *Baseline) Update(value float64, ts time.Time) {
	b.Count++
	if b.Count == 1 {
		b.Mean = value
		b.M2 = 0
		b.Min = value
		b.Max = value
	} else {
		delta := value - b.Mean
		b.Mean += delta / float64(b.Count)
		delta2 := value - b.Mean
		b.M2 += delta * delta2

		if value < b.Min {
			b.Min = value
		}
		if value > b.Max {
			b.Max = value
		}
	}
	b.LastSeen = ts
}

// Variance returns the population variance.
func (b *Baseline) Variance() float64 {
	if b.Count < 2 {
		return 0
	}
	return b.M2 / float64(b.Count)
}

// StdDev returns the population standard deviation.
func (b *Baseline) StdDev() float64 {
	v := b.Variance()
	if v <= 0 {
		return 0
	}
	return sqrt(v)
}

// HourOfWeekSlot returns the seasonal slot index [0..167] for a given UTC timestamp.
// Slot = (weekday * 24) + hour. Monday=0 .. Sunday=6.
func HourOfWeekSlot(t time.Time) int {
	t = t.UTC()
	// Go: Sunday=0, Monday=1 ... Saturday=6
	// We want Monday=0 ... Sunday=6
	dow := int(t.Weekday())
	if dow == 0 {
		dow = 6 // Sunday -> 6
	} else {
		dow-- // Monday=0 .. Saturday=5
	}
	return dow*24 + t.Hour()
}

// SeasonalBaselines holds 168 hour-of-week baseline slots for a single metric key.
type SeasonalBaselines struct {
	MetricKey string       `json:"metric_key"`
	Slots     [168]Baseline `json:"slots"`
}

func NewSeasonalBaselines(metricKey string) *SeasonalBaselines {
	return &SeasonalBaselines{MetricKey: metricKey}
}

func (sb *SeasonalBaselines) Update(value float64, ts time.Time) {
	slot := HourOfWeekSlot(ts)
	sb.Slots[slot].Update(value, ts)
}

func (sb *SeasonalBaselines) Get(ts time.Time) *Baseline {
	slot := HourOfWeekSlot(ts)
	return &sb.Slots[slot]
}

// sqrt is a simple float64 square root to avoid importing math just for this.
func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 100; i++ {
		z = (z + x/z) / 2
	}
	return z
}
