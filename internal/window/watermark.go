package window

import (
	"sync"
	"time"
)

// WatermarkTracker tracks the monotonically advancing event-time watermark per metric key.
type WatermarkTracker struct {
	mu             sync.RWMutex
	maxObserved    time.Time
	allowedLateness time.Duration
}

func NewWatermarkTracker(allowedLateness time.Duration) *WatermarkTracker {
	return &WatermarkTracker{
		allowedLateness: allowedLateness,
	}
}

// Update processes an incoming event's timestamp, advancing the max observed time monotonically.
// Returns the current event-time watermark.
func (w *WatermarkTracker) Update(eventTime time.Time) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()

	if eventTime.After(w.maxObserved) {
		w.maxObserved = eventTime
	}

	if w.maxObserved.IsZero() {
		return time.Time{}
	}

	return w.maxObserved.Add(-w.allowedLateness)
}

// Watermark returns the current watermark timestamp.
func (w *WatermarkTracker) Watermark() time.Time {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.maxObserved.IsZero() {
		return time.Time{}
	}
	return w.maxObserved.Add(-w.allowedLateness)
}

// IsLate checks if an event with timestamp t is strictly behind the current watermark.
func (w *WatermarkTracker) IsLate(t time.Time) bool {
	wm := w.Watermark()
	if wm.IsZero() {
		return false
	}
	return t.Before(wm)
}
