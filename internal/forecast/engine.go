package forecast

import (
	"fmt"
	"math"
	"time"
)

// Forecaster is the pluggable interface for time-series forecasting algorithms.
type Forecaster interface {
	// Fit trains the model on historical observations.
	Fit(observations []Observation) error

	// Predict returns forecasted values for the given horizon (number of future periods).
	Predict(horizon int) ([]ForecastPoint, error)

	// Name returns the algorithm identifier.
	Name() string
}

// Observation represents a historical data point for training.
type Observation struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
}

// ForecastPoint represents a single predicted future value.
type ForecastPoint struct {
	Timestamp time.Time `json:"timestamp"`
	Value     float64   `json:"value"`
	Lower     float64   `json:"lower"` // Lower confidence bound
	Upper     float64   `json:"upper"` // Upper confidence bound
}

// SeasonalNaiveForecaster uses the value from the same hour last week as the forecast.
// Simple, surprisingly effective baseline for strongly seasonal time series.
type SeasonalNaiveForecaster struct {
	seasonalityPeriod int           // Number of periods in one season (e.g. 168 for hourly data, weekly cycle)
	interval          time.Duration // Time between consecutive observations
	history           []Observation
}

func NewSeasonalNaiveForecaster(seasonalityPeriod int, interval time.Duration) *SeasonalNaiveForecaster {
	if seasonalityPeriod <= 0 {
		seasonalityPeriod = 168
	}
	if interval <= 0 {
		interval = time.Hour
	}
	return &SeasonalNaiveForecaster{
		seasonalityPeriod: seasonalityPeriod,
		interval:          interval,
	}
}

func (f *SeasonalNaiveForecaster) Name() string { return "seasonal_naive" }

func (f *SeasonalNaiveForecaster) Fit(observations []Observation) error {
	if len(observations) < f.seasonalityPeriod {
		return fmt.Errorf("seasonal_naive requires at least %d observations, got %d", f.seasonalityPeriod, len(observations))
	}
	f.history = make([]Observation, len(observations))
	copy(f.history, observations)
	return nil
}

func (f *SeasonalNaiveForecaster) Predict(horizon int) ([]ForecastPoint, error) {
	if len(f.history) < f.seasonalityPeriod {
		return nil, fmt.Errorf("model not fitted or insufficient history")
	}

	n := len(f.history)
	lastTime := f.history[n-1].Timestamp

	// Compute residual standard deviation for confidence intervals
	residualStd := f.computeResidualStd()

	points := make([]ForecastPoint, horizon)
	for i := 0; i < horizon; i++ {
		// Look back exactly one season
		srcIdx := n - f.seasonalityPeriod + (i % f.seasonalityPeriod)
		if srcIdx < 0 {
			srcIdx = 0
		}
		if srcIdx >= n {
			srcIdx = n - 1
		}

		val := f.history[srcIdx].Value
		ts := lastTime.Add(time.Duration(i+1) * f.interval)

		// Confidence interval widens with forecast horizon
		spread := residualStd * math.Sqrt(float64(i+1)/float64(f.seasonalityPeriod)) * 1.96

		points[i] = ForecastPoint{
			Timestamp: ts,
			Value:     val,
			Lower:     val - spread,
			Upper:     val + spread,
		}
	}

	return points, nil
}

func (f *SeasonalNaiveForecaster) computeResidualStd() float64 {
	n := len(f.history)
	if n <= f.seasonalityPeriod {
		return 0
	}

	var sumSq float64
	count := 0
	for i := f.seasonalityPeriod; i < n; i++ {
		residual := f.history[i].Value - f.history[i-f.seasonalityPeriod].Value
		sumSq += residual * residual
		count++
	}

	if count == 0 {
		return 0
	}
	return math.Sqrt(sumSq / float64(count))
}

// HoltWintersForecaster implements triple exponential smoothing (additive seasonality).
type HoltWintersForecaster struct {
	alpha             float64 // Level smoothing
	beta              float64 // Trend smoothing
	gamma             float64 // Seasonal smoothing
	seasonalityPeriod int
	interval          time.Duration

	// Fitted state
	level    float64
	trend    float64
	seasonal []float64
	lastTime time.Time
	fitted   bool
}

func NewHoltWintersForecaster(alpha, beta, gamma float64, seasonalityPeriod int, interval time.Duration) *HoltWintersForecaster {
	if alpha <= 0 || alpha >= 1 {
		alpha = 0.2
	}
	if beta <= 0 || beta >= 1 {
		beta = 0.1
	}
	if gamma <= 0 || gamma >= 1 {
		gamma = 0.3
	}
	if seasonalityPeriod <= 0 {
		seasonalityPeriod = 168
	}
	if interval <= 0 {
		interval = time.Hour
	}
	return &HoltWintersForecaster{
		alpha:             alpha,
		beta:              beta,
		gamma:             gamma,
		seasonalityPeriod: seasonalityPeriod,
		interval:          interval,
	}
}

func (hw *HoltWintersForecaster) Name() string { return "holt_winters" }

func (hw *HoltWintersForecaster) Fit(observations []Observation) error {
	m := hw.seasonalityPeriod
	n := len(observations)
	if n < 2*m {
		return fmt.Errorf("holt_winters requires at least %d observations (2 full seasons), got %d", 2*m, n)
	}

	// Initialize level as mean of first season
	var sum float64
	for i := 0; i < m; i++ {
		sum += observations[i].Value
	}
	hw.level = sum / float64(m)

	// Initialize trend as average difference between first two seasons
	hw.trend = 0
	for i := 0; i < m; i++ {
		hw.trend += (observations[m+i].Value - observations[i].Value)
	}
	hw.trend /= float64(m * m)

	// Initialize seasonal factors (additive)
	hw.seasonal = make([]float64, m)
	for i := 0; i < m; i++ {
		hw.seasonal[i] = observations[i].Value - hw.level
	}

	// Run smoothing updates over all observations
	for t := m; t < n; t++ {
		x := observations[t].Value
		seasonIdx := t % m

		prevLevel := hw.level
		hw.level = hw.alpha*(x-hw.seasonal[seasonIdx]) + (1-hw.alpha)*(prevLevel+hw.trend)
		hw.trend = hw.beta*(hw.level-prevLevel) + (1-hw.beta)*hw.trend
		hw.seasonal[seasonIdx] = hw.gamma*(x-hw.level) + (1-hw.gamma)*hw.seasonal[seasonIdx]
	}

	hw.lastTime = observations[n-1].Timestamp
	hw.fitted = true
	return nil
}

func (hw *HoltWintersForecaster) Predict(horizon int) ([]ForecastPoint, error) {
	if !hw.fitted {
		return nil, fmt.Errorf("model not fitted")
	}

	m := hw.seasonalityPeriod
	points := make([]ForecastPoint, horizon)

	for h := 1; h <= horizon; h++ {
		seasonIdx := h % m
		if seasonIdx < 0 {
			seasonIdx += m
		}
		if seasonIdx >= len(hw.seasonal) {
			seasonIdx = 0
		}

		val := hw.level + float64(h)*hw.trend + hw.seasonal[seasonIdx]
		ts := hw.lastTime.Add(time.Duration(h) * hw.interval)

		// Simple confidence interval based on horizon distance
		spread := math.Abs(hw.trend) * float64(h) * 1.96

		points[h-1] = ForecastPoint{
			Timestamp: ts,
			Value:     val,
			Lower:     val - spread,
			Upper:     val + spread,
		}
	}

	return points, nil
}
