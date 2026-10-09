package detector

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Baseline / Welford Tests ---

func TestBaseline_WelfordOnline(t *testing.T) {
	b := &Baseline{}
	values := []float64{10, 20, 30, 40, 50}

	for _, v := range values {
		b.Update(v, time.Now())
	}

	assert.Equal(t, int64(5), b.Count)
	assert.InDelta(t, 30.0, b.Mean, 0.0001)
	assert.InDelta(t, 10.0, b.Min, 0.0001)
	assert.InDelta(t, 50.0, b.Max, 0.0001)

	// Population variance of [10,20,30,40,50] = 200
	assert.InDelta(t, 200.0, b.Variance(), 0.0001)
	// Population stddev = sqrt(200) ≈ 14.142
	assert.InDelta(t, math.Sqrt(200.0), b.StdDev(), 0.01)
}

func TestHourOfWeekSlot(t *testing.T) {
	// Monday 3am UTC -> slot = 0*24 + 3 = 3
	mon3am := time.Date(2026, 10, 5, 3, 0, 0, 0, time.UTC) // Monday
	assert.Equal(t, 3, HourOfWeekSlot(mon3am))

	// Sunday 23:00 UTC -> slot = 6*24 + 23 = 167
	sun23 := time.Date(2026, 10, 11, 23, 0, 0, 0, time.UTC) // Sunday
	assert.Equal(t, 167, HourOfWeekSlot(sun23))

	// Friday 12:00 UTC -> slot = 4*24 + 12 = 108
	fri12 := time.Date(2026, 10, 9, 12, 0, 0, 0, time.UTC) // Thursday actually, let me check
	// Oct 9, 2026 is a Friday
	assert.Equal(t, 4*24+12, HourOfWeekSlot(fri12))
}

// --- EWMA + Z-Score Detector Tests ---

func TestEWMAZScore_NoAnomalyDuringWarmup(t *testing.T) {
	d := NewEWMAZScoreDetector(0.3, 3.0, 10)
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC) // Monday 10am

	// Feed 5 normal events (below min_observations=10)
	for i := 0; i < 5; i++ {
		anomalies, err := d.Detect(ctx, domain.Event{
			EventID:   "warmup-" + string(rune('a'+i)),
			MetricKey: "orders",
			Timestamp: baseTime.Add(time.Duration(i) * time.Minute),
			Value:     100.0,
		})
		require.NoError(t, err)
		assert.Empty(t, anomalies, "No anomalies during warmup phase")
	}
}

func TestEWMAZScore_DetectsSpike(t *testing.T) {
	d := NewEWMAZScoreDetector(0.3, 3.0, 5)
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC) // Monday 10am

	// Build baseline with 20 normal observations around 100 ± small noise
	for i := 0; i < 20; i++ {
		val := 100.0 + float64(i%5) - 2.0 // values 98..102
		_, err := d.Detect(ctx, domain.Event{
			EventID:   fmt.Sprintf("normal-%d", i),
			MetricKey: "orders",
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Value:     val,
		})
		require.NoError(t, err)
	}

	// Inject massive spike
	anomalies, err := d.Detect(ctx, domain.Event{
		EventID:   "spike-1",
		MetricKey: "orders",
		Timestamp: baseTime.Add(21 * time.Second),
		Value:     500.0, // Way above baseline mean ~100
	})
	require.NoError(t, err)
	require.NotEmpty(t, anomalies)
	assert.Equal(t, domain.AnomalyTypeSpike, anomalies[0].Type)
	assert.True(t, anomalies[0].Score > 3.0)
}

func TestEWMAZScore_DetectsDip(t *testing.T) {
	d := NewEWMAZScoreDetector(0.3, 3.0, 5)
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)

	// Build baseline around 1000
	for i := 0; i < 20; i++ {
		val := 1000.0 + float64(i%3)
		_, _ = d.Detect(ctx, domain.Event{
			EventID:   fmt.Sprintf("base-%d", i),
			MetricKey: "latency",
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Value:     val,
		})
	}

	// Inject severe dip
	anomalies, _ := d.Detect(ctx, domain.Event{
		EventID:   "dip-1",
		MetricKey: "latency",
		Timestamp: baseTime.Add(21 * time.Second),
		Value:     0.5,
	})
	require.NotEmpty(t, anomalies)
	assert.Equal(t, domain.AnomalyTypeDip, anomalies[0].Type)
	assert.True(t, anomalies[0].Score < -3.0)
}

// --- Page-Hinkley Drift Detector Tests ---

func TestPageHinkley_DetectsGradualDrift(t *testing.T) {
	d := NewPageHinkleyDetector(0.1, 15.0, 0.99)
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)

	// 50 stable observations around 100
	for i := 0; i < 50; i++ {
		_, _ = d.Detect(ctx, domain.Event{
			EventID:   fmt.Sprintf("stable-%d", i),
			MetricKey: "cost",
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Value:     100.0,
		})
	}

	// Gradual drift upward: 100 -> 105 -> 110 -> ...
	var driftDetected bool
	for i := 0; i < 200; i++ {
		driftedValue := 100.0 + float64(i)*0.5 // Gradual increase
		anomalies, _ := d.Detect(ctx, domain.Event{
			EventID:   fmt.Sprintf("drift-%d", i),
			MetricKey: "cost",
			Timestamp: baseTime.Add(time.Duration(50+i) * time.Second),
			Value:     driftedValue,
		})
		if len(anomalies) > 0 {
			assert.Equal(t, domain.AnomalyTypeDrift, anomalies[0].Type)
			driftDetected = true
			break
		}
	}
	assert.True(t, driftDetected, "Page-Hinkley must detect gradual upward drift")
}

func TestPageHinkley_NoDriftOnStableSignal(t *testing.T) {
	d := NewPageHinkleyDetector(0.1, 15.0, 0.99)
	ctx := context.Background()
	baseTime := time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)

	for i := 0; i < 200; i++ {
		anomalies, _ := d.Detect(ctx, domain.Event{
			EventID:   fmt.Sprintf("stable-%d", i),
			MetricKey: "stable_metric",
			Timestamp: baseTime.Add(time.Duration(i) * time.Second),
			Value:     50.0,
		})
		assert.Empty(t, anomalies, "Stable signal must not trigger drift")
	}
}

// --- Silence Detector Tests ---

func TestSilenceDetector_FiresAfterTimeout(t *testing.T) {
	d := NewSilenceDetector(100 * time.Millisecond)

	now := time.Now().UTC()
	d.RecordActivity("orders", now)

	// Immediately after: no silence
	anomalies := d.CheckAll(now.Add(50 * time.Millisecond))
	assert.Empty(t, anomalies)

	// After timeout: silence detected
	anomalies = d.CheckAll(now.Add(150 * time.Millisecond))
	require.Len(t, anomalies, 1)
	assert.Equal(t, domain.AnomalyTypeSilence, anomalies[0].Type)
	assert.Equal(t, "orders", anomalies[0].MetricKey)
}

func TestSilenceDetector_ResetsOnResume(t *testing.T) {
	d := NewSilenceDetector(100 * time.Millisecond)

	now := time.Now().UTC()
	d.RecordActivity("orders", now)

	// Silence fires
	anomalies := d.CheckAll(now.Add(150 * time.Millisecond))
	require.Len(t, anomalies, 1)

	// Won't fire again (already fired for this silence window)
	anomalies = d.CheckAll(now.Add(200 * time.Millisecond))
	assert.Empty(t, anomalies)

	// Data resumes -> resets the fired flag
	d.RecordActivity("orders", now.Add(250*time.Millisecond))

	// New silence after resume
	anomalies = d.CheckAll(now.Add(400 * time.Millisecond))
	require.Len(t, anomalies, 1)
}

// --- Cold Start Tests ---

func TestColdStart_InheritsGroupBaseline(t *testing.T) {
	cs := NewColdStartManager("region")

	// Register two stores in same region
	cs.Register("store_A", map[string]string{"region": "west"})
	cs.Register("store_B", map[string]string{"region": "west"})
	cs.Register("store_C", map[string]string{"region": "east"})

	// Store A contributes observations to the "west" group (slot 0)
	for i := 0; i < 20; i++ {
		cs.ContributeToGroupRaw("store_A", 100.0+float64(i), 0)
	}

	// Store B is new (cold start) and should inherit from "west" group
	inherited := cs.GetInheritedBaseline("store_B")
	require.NotNil(t, inherited)
	assert.Equal(t, int64(20), inherited.Slots[0].Count)
	assert.InDelta(t, 109.5, inherited.Slots[0].Mean, 0.1)

	// Store C is in "east" group and should NOT inherit "west" data
	inheritedEast := cs.GetInheritedBaseline("store_C")
	require.NotNil(t, inheritedEast)
	assert.Equal(t, int64(0), inheritedEast.Slots[0].Count)
}

// --- State Store Tests ---

func TestMemoryStateStore_RoundTrip(t *testing.T) {
	store := NewMemoryStateStore()
	ctx := context.Background()

	original := NewSeasonalBaselines("test_metric")
	for i := 0; i < 10; i++ {
		original.Slots[42].Update(float64(100+i), time.Now())
	}

	err := store.SaveBaselines(ctx, "test_metric", original)
	require.NoError(t, err)

	restored, err := store.LoadBaselines(ctx, "test_metric")
	require.NoError(t, err)
	require.NotNil(t, restored)

	assert.Equal(t, original.Slots[42].Count, restored.Slots[42].Count)
	assert.InDelta(t, original.Slots[42].Mean, restored.Slots[42].Mean, 0.0001)
	assert.InDelta(t, original.Slots[42].M2, restored.Slots[42].M2, 0.0001)
}
