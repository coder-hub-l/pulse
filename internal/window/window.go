package window

import (
	"math"
	"sort"
	"sync"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
)

// Bucket represents an active aggregation interval for a specific resolution.
type Bucket struct {
	MetricKey   string
	Resolution  string
	WindowStart time.Time
	WindowEnd   time.Time
	Count       uint64
	Sum         float64
	Min         float64
	Max         float64
	Values      []float64
	Dimensions  map[string]string
}

func newBucket(metricKey, resolution string, start, end time.Time, dims map[string]string) *Bucket {
	return &Bucket{
		MetricKey:   metricKey,
		Resolution:  resolution,
		WindowStart: start,
		WindowEnd:   end,
		Min:         math.MaxFloat64,
		Max:         -math.MaxFloat64,
		Values:      make([]float64, 0, 64),
		Dimensions:  dims,
	}
}

func (b *Bucket) Add(value float64) {
	b.Count++
	b.Sum += value
	if value < b.Min {
		b.Min = value
	}
	if value > b.Max {
		b.Max = value
	}
	b.Values = append(b.Values, value)
}

func (b *Bucket) ToRollup() domain.Rollup {
	r := domain.Rollup{
		MetricKey:        b.MetricKey,
		WindowResolution: b.Resolution,
		WindowStart:      b.WindowStart,
		WindowEnd:        b.WindowEnd,
		EventCount:       b.Count,
		SumValue:         b.Sum,
		MinValue:         b.Min,
		MaxValue:         b.Max,
		Dimensions:       b.Dimensions,
		CreatedAt:        time.Now().UTC(),
	}

	if b.Count > 0 {
		r.MeanValue = b.Sum / float64(b.Count)
		sort.Float64s(b.Values)
		r.P50Value = percentile(b.Values, 0.50)
		r.P90Value = percentile(b.Values, 0.90)
		r.P99Value = percentile(b.Values, 0.99)
	} else {
		r.MinValue = 0
		r.MaxValue = 0
	}

	return r
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	idx := p * float64(len(sorted)-1)
	low := int(math.Floor(idx))
	high := int(math.Ceil(idx))
	if low == high {
		return sorted[low]
	}
	weight := idx - float64(low)
	return sorted[low]*(1-weight) + sorted[high]*weight
}

// WindowAggregator manages tumbling window state across multiple resolutions per metric key.
type WindowAggregator struct {
	mu              sync.Mutex
	metricKey       string
	resolutions     []time.Duration
	resolutionNames []string
	watermark       *WatermarkTracker
	allowedLateness time.Duration
	buckets         map[string]map[int64]*Bucket // resolutionName -> startUnix -> Bucket
}

func NewWindowAggregator(metricKey string, resolutionDurations []time.Duration, resolutionNames []string, allowedLateness time.Duration) *WindowAggregator {
	buckets := make(map[string]map[int64]*Bucket)
	for _, name := range resolutionNames {
		buckets[name] = make(map[int64]*Bucket)
	}

	return &WindowAggregator{
		metricKey:       metricKey,
		resolutions:     resolutionDurations,
		resolutionNames: resolutionNames,
		watermark:       NewWatermarkTracker(allowedLateness),
		allowedLateness: allowedLateness,
		buckets:         buckets,
	}
}

// Ingest adds an event to the aggregator.
// Returns isLate=true if the event arrived after the allowed lateness watermark (DLQ candidate).
// Also returns any closed window rollups triggered by watermark advancement.
func (wa *WindowAggregator) Ingest(event domain.Event) (closedRollups []domain.Rollup, isDLQ bool) {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	// Advance watermark with the new event
	wm := wa.watermark.Update(event.Timestamp)

	// Check if this event is older than the allowed-lateness watermark
	if !wm.IsZero() && event.Timestamp.Before(wm) {
		// Beyond allowed lateness -> Send to DLQ
		return wa.collectClosedWindows(wm), true
	}

	// Update buckets across all configured resolutions
	for i, resDur := range wa.resolutions {
		resName := wa.resolutionNames[i]
		windowStart := alignToWindow(event.Timestamp, resDur)
		windowEnd := windowStart.Add(resDur)
		startUnix := windowStart.Unix()

		b, exists := wa.buckets[resName][startUnix]
		if !exists {
			b = newBucket(wa.metricKey, resName, windowStart, windowEnd, event.Dimensions)
			wa.buckets[resName][startUnix] = b
		}
		b.Add(event.Value)
	}

	// Evict and seal windows that are fully behind the updated watermark
	closedRollups = wa.collectClosedWindows(wm)
	return closedRollups, false
}

// FlushAll forces all active and open buckets to be sealed into Rollups (e.g., during shutdown).
func (wa *WindowAggregator) FlushAll() []domain.Rollup {
	wa.mu.Lock()
	defer wa.mu.Unlock()

	var allRollups []domain.Rollup
	for resName, bucketMap := range wa.buckets {
		for startUnix, b := range bucketMap {
			if b.Count > 0 {
				allRollups = append(allRollups, b.ToRollup())
			}
			delete(bucketMap, startUnix)
		}
		wa.buckets[resName] = bucketMap
	}
	return allRollups
}

func (wa *WindowAggregator) collectClosedWindows(wm time.Time) []domain.Rollup {
	if wm.IsZero() {
		return nil
	}

	var closed []domain.Rollup
	for resName, bucketMap := range wa.buckets {
		for startUnix, b := range bucketMap {
			// Window is closed when its end time is <= watermark
			if !b.WindowEnd.After(wm) {
				if b.Count > 0 {
					closed = append(closed, b.ToRollup())
				}
				delete(bucketMap, startUnix)
			}
		}
		wa.buckets[resName] = bucketMap
	}
	return closed
}

func alignToWindow(t time.Time, duration time.Duration) time.Time {
	return t.Truncate(duration)
}
