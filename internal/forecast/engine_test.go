package forecast

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateSeasonalData(periods, seasonLength int, baseValue, amplitude float64) []Observation {
	total := periods * seasonLength
	obs := make([]Observation, total)
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := 0; i < total; i++ {
		seasonal := amplitude * math.Sin(2*math.Pi*float64(i%seasonLength)/float64(seasonLength))
		obs[i] = Observation{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Value:     baseValue + seasonal,
		}
	}
	return obs
}

func TestSeasonalNaive_FitAndPredict(t *testing.T) {
	// 3 weeks of hourly data (168 * 3 = 504 points)
	obs := generateSeasonalData(3, 168, 100.0, 20.0)

	f := NewSeasonalNaiveForecaster(168, time.Hour)
	err := f.Fit(obs)
	require.NoError(t, err)

	// Forecast 24 hours ahead
	points, err := f.Predict(24)
	require.NoError(t, err)
	assert.Len(t, points, 24)

	// Each forecast should be very close to the value from 168 hours (1 week) ago
	n := len(obs)
	for i, pt := range points {
		srcIdx := n - 168 + i
		expected := obs[srcIdx].Value
		assert.InDelta(t, expected, pt.Value, 0.0001, "Seasonal naive should replicate last week's value at hour %d", i)
		assert.True(t, pt.Lower <= pt.Value)
		assert.True(t, pt.Upper >= pt.Value)
	}
}

func TestSeasonalNaive_InsufficientData(t *testing.T) {
	f := NewSeasonalNaiveForecaster(168, time.Hour)
	err := f.Fit(make([]Observation, 100)) // Less than 168
	assert.Error(t, err)
}

func TestHoltWinters_FitAndPredict(t *testing.T) {
	// 4 weeks of hourly data
	obs := generateSeasonalData(4, 168, 200.0, 30.0)

	hw := NewHoltWintersForecaster(0.2, 0.1, 0.3, 168, time.Hour)
	err := hw.Fit(obs)
	require.NoError(t, err)

	points, err := hw.Predict(48)
	require.NoError(t, err)
	assert.Len(t, points, 48)

	// Forecasts should be in a reasonable range around the base value
	for _, pt := range points {
		assert.True(t, pt.Value > 100.0 && pt.Value < 300.0,
			"Forecast value %.2f should be in reasonable range", pt.Value)
		assert.True(t, pt.Lower <= pt.Value)
		assert.True(t, pt.Upper >= pt.Value)
	}
}

func TestHoltWinters_InsufficientData(t *testing.T) {
	hw := NewHoltWintersForecaster(0.2, 0.1, 0.3, 168, time.Hour)
	err := hw.Fit(make([]Observation, 200)) // Less than 336 (2 seasons)
	assert.Error(t, err)
}

func TestHoltWinters_CaptursTrend(t *testing.T) {
	// Generate data with upward trend + seasonality
	obs := make([]Observation, 4*24) // 4 days of hourly data, season=24
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < len(obs); i++ {
		trend := float64(i) * 0.5 // Upward trend
		seasonal := 10.0 * math.Sin(2*math.Pi*float64(i%24)/24.0)
		obs[i] = Observation{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Value:     100.0 + trend + seasonal,
		}
	}

	hw := NewHoltWintersForecaster(0.3, 0.1, 0.3, 24, time.Hour)
	err := hw.Fit(obs)
	require.NoError(t, err)

	points, err := hw.Predict(48) // Forecast 2 full seasons (48 hours)
	require.NoError(t, err)

	// With positive trend, the average forecast over the second full season (hours 25-48)
	// should exceed the average over the first full season (hours 1-24).
	// This removes seasonal oscillation from the comparison.
	var avgFirst, avgSecond float64
	for i := 0; i < 24; i++ {
		avgFirst += points[i].Value
		avgSecond += points[24+i].Value
	}
	avgFirst /= 24.0
	avgSecond /= 24.0

	assert.True(t, avgSecond > avgFirst,
		"Holt-Winters with positive trend: avg season2 (%.2f) should exceed avg season1 (%.2f)",
		avgSecond, avgFirst)
}
