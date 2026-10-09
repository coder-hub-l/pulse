package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/pulse/pulse-engine/internal/forecast"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	port := os.Getenv("FORECAST_PORT")
	if port == "" {
		port = "8082"
	}

	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/forecast", handleForecast)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})

	server := &http.Server{
		Addr:         ":" + port,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		logger.Info("Forecast API listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Forecast server failed", "err", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("Forecast API shutdown")
}

func handleForecast(w http.ResponseWriter, r *http.Request) {
	metricKey := r.URL.Query().Get("key")
	if metricKey == "" {
		http.Error(w, `{"error":"missing required query param: key"}`, http.StatusBadRequest)
		return
	}

	horizonStr := r.URL.Query().Get("horizon")
	if horizonStr == "" {
		horizonStr = "24"
	}
	horizon, err := strconv.Atoi(horizonStr)
	if err != nil || horizon <= 0 || horizon > 720 {
		http.Error(w, `{"error":"horizon must be an integer between 1 and 720"}`, http.StatusBadRequest)
		return
	}

	algorithm := r.URL.Query().Get("algorithm")
	if algorithm == "" {
		algorithm = "holt_winters"
	}

	// Generate synthetic historical data for demo (in production, query ClickHouse rollups)
	history := generateDemoHistory(metricKey, 4*168) // 4 weeks

	var forecaster forecast.Forecaster
	switch algorithm {
	case "seasonal_naive":
		forecaster = forecast.NewSeasonalNaiveForecaster(168, time.Hour)
	case "holt_winters":
		forecaster = forecast.NewHoltWintersForecaster(0.2, 0.1, 0.3, 168, time.Hour)
	default:
		http.Error(w, fmt.Sprintf(`{"error":"unsupported algorithm: %s"}`, algorithm), http.StatusBadRequest)
		return
	}

	if err := forecaster.Fit(history); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"fit failed: %s"}`, err.Error()), http.StatusUnprocessableEntity)
		return
	}

	points, err := forecaster.Predict(horizon)
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"predict failed: %s"}`, err.Error()), http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"metric_key": metricKey,
		"algorithm":  algorithm,
		"horizon":    horizon,
		"points":     points,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func generateDemoHistory(metricKey string, numPoints int) []forecast.Observation {
	obs := make([]forecast.Observation, numPoints)
	baseTime := time.Now().UTC().Add(-time.Duration(numPoints) * time.Hour)

	for i := 0; i < numPoints; i++ {
		hourOfWeek := i % 168
		// Simulate daily pattern: peak at hour 12, trough at hour 3
		dailyHour := hourOfWeek % 24
		dailyEffect := 50.0 * math.Sin(2*math.Pi*float64(dailyHour-6)/24.0)

		// Weekend effect: lower on Saturday (day 5) and Sunday (day 6)
		dayOfWeek := hourOfWeek / 24
		weekendFactor := 1.0
		if dayOfWeek >= 5 {
			weekendFactor = 0.7
		}

		obs[i] = forecast.Observation{
			Timestamp: baseTime.Add(time.Duration(i) * time.Hour),
			Value:     200.0*weekendFactor + dailyEffect,
		}
	}
	return obs
}
