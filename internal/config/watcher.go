package config

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Watcher periodically polls the metrics directory for changes and hot-reloads configurations atomically.
type Watcher struct {
	manager  *ConfigManager
	interval time.Duration
	dir      string
	lastMod  map[string]time.Time
	mu       sync.Mutex
	logger   *slog.Logger
}

func NewWatcher(manager *ConfigManager, dir string, interval time.Duration, logger *slog.Logger) *Watcher {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Watcher{
		manager:  manager,
		interval: interval,
		dir:      dir,
		lastMod:  make(map[string]time.Time),
		logger:   logger,
	}
}

// Start begins the polling loop in a goroutine until the context is canceled.
func (w *Watcher) Start(ctx context.Context) {
	w.scanAndReload() // initial scan

	ticker := time.NewTicker(w.interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				w.logger.Info("Config watcher stopped")
				return
			case <-ticker.C:
				w.scanAndReload()
			}
		}
	}()
}

func (w *Watcher) scanAndReload() {
	w.mu.Lock()
	defer w.mu.Unlock()

	entries, err := os.ReadDir(w.dir)
	if err != nil {
		w.logger.Error("Failed to read metrics directory for watcher", "dir", w.dir, "err", err)
		return
	}

	hasChanges := false
	currentMods := make(map[string]time.Time)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := filepath.Ext(entry.Name())
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		currentMods[entry.Name()] = info.ModTime()
		if lastTime, exists := w.lastMod[entry.Name()]; !exists || !lastTime.Equal(info.ModTime()) {
			hasChanges = true
		}
	}

	if len(currentMods) != len(w.lastMod) {
		hasChanges = true
	}

	if !hasChanges {
		return
	}

	w.logger.Info("Detected changes in metric configs, reloading...", "dir", w.dir)
	metrics, err := LoadMetricsDir(w.dir)
	if err != nil {
		w.logger.Error("Failed to reload metric configs, retaining existing active config", "err", err)
		return
	}

	currentCfg := w.manager.Get()
	if currentCfg != nil {
		newCfg := *currentCfg
		newCfg.Metrics = metrics
		if err := newCfg.Validate(); err != nil {
			w.logger.Error("Reloaded config validation failed, retaining existing active config", "err", err)
			return
		}
		w.manager.Set(&newCfg)
	}

	w.lastMod = currentMods
	w.logger.Info("Successfully hot-reloaded metric configs", "total_metrics", len(metrics))
}
