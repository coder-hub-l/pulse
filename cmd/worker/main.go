package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pulse/pulse-engine/internal/alert"
	"github.com/pulse/pulse-engine/internal/config"
	"github.com/pulse/pulse-engine/internal/dedup"
	"github.com/pulse/pulse-engine/internal/queue"
	"github.com/pulse/pulse-engine/internal/storage"
	"github.com/pulse/pulse-engine/internal/window"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	logger.Info("Starting Pulse Streaming Worker Engine...")

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

	// Hot-reload watcher
	watcher := config.NewWatcher(cfgManager, cfg.Server.MetricsDir, 5*time.Second, logger)
	watcher.Start(ctx)

	// Redis Dedup
	var deduplicator dedup.Deduplicator
	redisClient, err := storage.NewRedisClient(ctx, cfg.Server.RedisURL)
	if err != nil {
		logger.Warn("Failed to connect to Redis, using in-memory deduplicator", "err", err)
		deduplicator = dedup.NewMemoryDeduplicator()
	} else {
		deduplicator = dedup.NewRedisDeduplicator(redisClient.Client, "pulse:dedup")
	}

	// ClickHouse Rollup Writer
	var clickhouseClient *storage.ClickHouseClient
	chClient, err := storage.NewClickHouseClient(ctx, cfg.Server.ClickHouseDSN)
	if err != nil {
		logger.Warn("Failed to connect to ClickHouse, running in memory-buffer mode", "err", err)
	} else {
		clickhouseClient = chClient
	}

	// Redpanda DLQ Producer
	var dlqProducer queue.Producer
	p, err := queue.NewRedpandaProducer(cfg.Server.RedpandaBrokers)
	if err != nil {
		logger.Warn("Redpanda producer unavailable, using mock DLQ", "err", err)
		dlqProducer = queue.NewMockProducer()
	} else {
		dlqProducer = p
	}

	// Alert Engine & Sinks
	var sinks []alert.Sink
	if webhookURL := os.Getenv("ALERT_WEBHOOK_URL"); webhookURL != "" {
		sinks = append(sinks, alert.NewWebhookSink(webhookURL, logger))
	}
	if slackURL := os.Getenv("SLACK_WEBHOOK_URL"); slackURL != "" {
		sinks = append(sinks, alert.NewSlackSink(slackURL, logger))
	}
	if len(sinks) == 0 {
		sinks = append(sinks, alert.NewMemorySink()) // Default test/dry-run sink
	}
	alertEngine := alert.NewEngine(5*time.Minute, []string{"metric_key"}, sinks...)

	// Streaming Processor
	processor := window.NewPipelineProcessor(cfgManager, deduplicator, clickhouseClient, dlqProducer, logger)
	processor.SetAlertEngine(alertEngine)

	// Redpanda Consumer Group
	var consumer queue.Consumer
	consumerGroup := os.Getenv("WORKER_GROUP_ID")
	if consumerGroup == "" {
		consumerGroup = "pulse-workers-v1"
	}

	c, err := queue.NewRedpandaConsumer(cfg.Server.RedpandaBrokers, consumerGroup, []string{cfg.Server.RawEventsTopic}, logger)
	if err != nil {
		logger.Warn("Redpanda consumer unavailable, starting mock stream listener", "err", err)
		consumer = queue.NewMockConsumer()
	} else {
		consumer = c
	}

	// Run consumer loop in background
	go func() {
		if err := consumer.Start(ctx, processor.ProcessEvent); err != nil && err != context.Canceled {
			logger.Error("Consumer stopped with error", "err", err)
		}
	}()

	// Signal handling & Graceful Shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Worker shutdown initiated. Flushing open window buffers to ClickHouse...")
	cancel() // Stop consumer

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer flushCancel()

	if err := processor.FlushAll(flushCtx); err != nil {
		logger.Error("Error during shutdown window flush", "err", err)
	}

	_ = consumer.Close()
	_ = dlqProducer.Close()
	if clickhouseClient != nil {
		_ = clickhouseClient.Close()
	}

	logger.Info("Pulse Streaming Worker terminated cleanly")
}
