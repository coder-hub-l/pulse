package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/ingest"
	"github.com/pulse/pulse-engine/internal/queue"
	pulsev1 "github.com/pulse/pulse-engine/pkg/proto/pulse/v1"
	"google.golang.org/grpc"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logger.Info("Starting Pulse Ingest API Gateway...")

	serverConfigPath := os.Getenv("PULSE_CONFIG")
	if serverConfigPath == "" {
		serverConfigPath = "configs/pulse.yaml"
	}
	metricsDir := os.Getenv("METRICS_DIR")
	if metricsDir == "" {
		metricsDir = "configs/metrics"
	}

	cfg, err := config.Load(serverConfigPath, metricsDir)
	if err != nil {
		logger.Error("Failed to load initial configuration", "err", err)
		os.Exit(1)
	}

	cfgManager := config.NewConfigManager()
	cfgManager.Set(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start config hot-reloading watcher
	watcher := config.NewWatcher(cfgManager, cfg.Server.MetricsDir, 5*time.Second, logger)
	watcher.Start(ctx)

	// Initialize Redpanda Producer
	var producer queue.Producer
	if os.Getenv("MOCK_QUEUE") == "true" {
		logger.Info("Using in-memory mock queue producer")
		producer = queue.NewMockProducer()
	} else {
		logger.Info("Connecting to Redpanda brokers", "brokers", cfg.Server.RedpandaBrokers)
		p, err := queue.NewRedpandaProducer(cfg.Server.RedpandaBrokers)
		if err != nil {
			logger.Warn("Failed to connect to Redpanda, falling back to mock producer for offline development", "err", err)
			producer = queue.NewMockProducer()
		} else {
			producer = p
		}
	}
	defer producer.Close()

	// Initialize HTTP Ingest Router
	httpHandler := ingest.NewHTTPHandler(cfgManager, producer, logger)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/events", httpHandler.HandleIngestEvent)
	mux.HandleFunc("POST /v1/events/batch", httpHandler.HandleIngestBatch)
	mux.HandleFunc("GET /healthz", httpHandler.HandleHealthz)
	mux.Handle("GET /metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Server.HTTPPort),
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
		IdleTimeout:  30 * time.Second,
	}

	go func() {
		logger.Info("HTTP Ingest Server listening", "port", cfg.Server.HTTPPort)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP Server failed", "err", err)
		}
	}()

	// Initialize gRPC Ingest Server
	grpcServer := grpc.NewServer()
	grpcHandler := ingest.NewGRPCHandler(cfgManager, producer)
	pulsev1.RegisterIngestServiceServer(grpcServer, grpcHandler)

	grpcLis, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Server.GRPCPort))
	if err != nil {
		logger.Error("Failed to bind gRPC port", "port", cfg.Server.GRPCPort, "err", err)
	} else {
		go func() {
			logger.Info("gRPC Ingest Server listening", "port", cfg.Server.GRPCPort)
			if err := grpcServer.Serve(grpcLis); err != nil {
				logger.Error("gRPC Server failed", "err", err)
			}
		}()
	}

	// Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Initiating graceful shutdown...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server forced shutdown", "err", err)
	}
	grpcServer.GracefulStop()

	logger.Info("Pulse Ingest API terminated cleanly")
}
