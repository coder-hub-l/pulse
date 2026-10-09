package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

var (
	ErrEmptyConfigPath   = errors.New("config path cannot be empty")
	ErrNoMetricsFound    = errors.New("no valid metric configurations found")
	ErrInvalidWindowSize = errors.New("window sizes must be non-empty and valid duration strings (e.g., '1m', '5m', '1h')")
	ErrInvalidLateness   = errors.New("allowed_lateness_sec cannot be negative")
	ErrInvalidDedupTTL   = errors.New("dedup_ttl_sec must be positive")
	ErrInvalidAlpha      = errors.New("detector alpha must be in (0, 1]")
	ErrInvalidZThreshold = errors.New("detector z_threshold must be positive")
)

type WindowConfig struct {
	TumblingSizes      []string `yaml:"tumbling_sizes"`      // e.g. ["1m", "5m", "1h"]
	AllowedLatenessSec int      `yaml:"allowed_lateness_sec"` // Max out-of-order latency before DLQ
	DedupTTLSec        int      `yaml:"dedup_ttl_sec"`        // Redis dedup TTL
}

type ColdStartConfig struct {
	InheritFromDimension string `yaml:"inherit_from_dimension"` // e.g. "region", "category"
}

type BaselinesConfig struct {
	SeasonalType    string          `yaml:"seasonal_type"`    // e.g. "hour_of_week" (168 slots)
	MinObservations int             `yaml:"min_observations"` // Min points before baseline is active
	ColdStart       ColdStartConfig `yaml:"cold_start"`
}

type SpikeDetectorConfig struct {
	Enabled    bool    `yaml:"enabled"`
	Algorithm  string  `yaml:"algorithm"`   // "ewma_zscore"
	Alpha      float64 `yaml:"alpha"`       // Smoothing factor (0 < alpha <= 1)
	ZThreshold float64 `yaml:"z_threshold"` // |z| threshold for alert
}

type DriftDetectorConfig struct {
	Enabled   bool    `yaml:"enabled"`
	Algorithm string  `yaml:"algorithm"` // "page_hinkley"
	Delta     float64 `yaml:"delta"`     // Minimum detectable shift
	Threshold float64 `yaml:"threshold"` // Cumulative decision threshold
	Alpha     float64 `yaml:"alpha"`     // Forgetting factor
}

type SilenceDetectorConfig struct {
	Enabled             bool `yaml:"enabled"`
	HeartbeatTimeoutSec int  `yaml:"heartbeat_timeout_sec"` // "no data" timeout
}

type DetectorsConfig struct {
	Spike   SpikeDetectorConfig   `yaml:"spike"`
	Drift   DriftDetectorConfig   `yaml:"drift"`
	Silence SilenceDetectorConfig `yaml:"silence"`
}

type AlertSinkConfig struct {
	Type string `yaml:"type"` // "webhook", "slack_webhook"
	URL  string `yaml:"url"`
}

type AlertingConfig struct {
	Severity    string            `yaml:"severity"` // "info", "warning", "critical"
	CooldownSec int               `yaml:"cooldown_sec"`
	GroupBy     []string          `yaml:"group_by"`
	Sinks       []AlertSinkConfig `yaml:"sinks"`
}

type ForecastingConfig struct {
	DefaultAlgorithm  string `yaml:"default_algorithm"`  // "seasonal_naive", "holt_winters"
	SeasonalityPeriod int    `yaml:"seasonality_period"` // e.g. 168 (hours/week)
}

type MetricRule struct {
	Description string            `yaml:"description"`
	Window      WindowConfig      `yaml:"window"`
	Baselines   BaselinesConfig   `yaml:"baselines"`
	Detectors   DetectorsConfig   `yaml:"detectors"`
	Alerting    AlertingConfig    `yaml:"alerting"`
	Forecasting ForecastingConfig `yaml:"forecasting"`
}

func (m *MetricRule) PrimaryWindowDuration() time.Duration {
	if len(m.Window.TumblingSizes) > 0 {
		d, err := ParseDuration(m.Window.TumblingSizes[0])
		if err == nil {
			return d
		}
	}
	return time.Minute
}

func (m *MetricRule) AllowedLateness() time.Duration {
	return time.Duration(m.Window.AllowedLatenessSec) * time.Second
}

func (m *MetricRule) DedupTTL() time.Duration {
	return time.Duration(m.Window.DedupTTLSec) * time.Second
}

func (m *MetricRule) SilenceTimeout() time.Duration {
	return time.Duration(m.Detectors.Silence.HeartbeatTimeoutSec) * time.Second
}

func (m *MetricRule) AlertCooldown() time.Duration {
	return time.Duration(m.Alerting.CooldownSec) * time.Second
}

type MetricFile struct {
	Version   string                `yaml:"version"`
	Namespace string                `yaml:"namespace"`
	Metrics   map[string]MetricRule `yaml:"metrics"`
}

type ServerConfig struct {
	HTTPPort               int      `yaml:"http_port"`
	GRPCPort               int      `yaml:"grpc_port"`
	RedpandaBrokers        []string `yaml:"redpanda_brokers"`
	RawEventsTopic         string   `yaml:"raw_events_topic"`
	DLQTopic               string   `yaml:"dlq_topic"`
	AlertsTopic            string   `yaml:"alerts_topic"`
	ClickHouseDSN          string   `yaml:"clickhouse_dsn"`
	RedisURL               string   `yaml:"redis_url"`
	IngestMaxFutureSkewSec int      `yaml:"ingest_max_future_skew_sec"`
	IngestMaxPastSkewSec   int      `yaml:"ingest_max_past_skew_sec"`
	MetricsDir             string   `yaml:"metrics_dir"`
	FlushBatchSize         int      `yaml:"flush_batch_size"`
	FlushIntervalMs        int      `yaml:"flush_interval_ms"`
}

type Config struct {
	Server  ServerConfig          `yaml:"server"`
	Metrics map[string]MetricRule `yaml:"-"`
}

// ConfigManager holds the atomic config pointer for zero-lock hot-reloading.
type ConfigManager struct {
	cfg atomic.Pointer[Config]
}

func NewConfigManager() *ConfigManager {
	return &ConfigManager{}
}

func (cm *ConfigManager) Get() *Config {
	return cm.cfg.Load()
}

func (cm *ConfigManager) Set(c *Config) {
	cm.cfg.Store(c)
}

// Load loads the main server config and all metric YAMLs from the metrics directory.
func Load(serverConfigPath, metricsDir string) (*Config, error) {
	var cfg Config

	if serverConfigPath != "" {
		data, err := os.ReadFile(serverConfigPath)
		if err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to read server config %q: %w", serverConfigPath, err)
		}
		if len(data) > 0 {
			if err := yaml.Unmarshal(data, &cfg); err != nil {
				return nil, fmt.Errorf("failed to parse server config YAML: %w", err)
			}
		}
	}

	applyServerDefaults(&cfg.Server)
	if metricsDir != "" {
		cfg.Server.MetricsDir = metricsDir
	}

	metrics, err := LoadMetricsDir(cfg.Server.MetricsDir)
	if err != nil {
		return nil, err
	}
	cfg.Metrics = metrics

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &cfg, nil
}

// LoadMetricsDir scans a directory for all .yaml and .yml files and aggregates metric rules.
func LoadMetricsDir(dir string) (map[string]MetricRule, error) {
	if dir == "" {
		return nil, ErrEmptyConfigPath
	}

	allMetrics := make(map[string]MetricRule)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read metrics directory %q: %w", dir, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(entry.Name()))
		if ext != ".yaml" && ext != ".yml" {
			continue
		}

		filePath := filepath.Join(dir, entry.Name())
		data, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read metric file %q: %w", filePath, err)
		}

		var mf MetricFile
		if err := yaml.Unmarshal(data, &mf); err != nil {
			return nil, fmt.Errorf("failed to parse metric file %q: %w", filePath, err)
		}

		for key, rule := range mf.Metrics {
			applyMetricDefaults(&rule)
			allMetrics[key] = rule
		}
	}

	if len(allMetrics) == 0 {
		return nil, ErrNoMetricsFound
	}

	return allMetrics, nil
}

func applyServerDefaults(s *ServerConfig) {
	if s.HTTPPort == 0 {
		s.HTTPPort = 8080
	}
	if s.GRPCPort == 0 {
		s.GRPCPort = 9090
	}
	if len(s.RedpandaBrokers) == 0 {
		brokers := os.Getenv("REDPANDA_BROKERS")
		if brokers != "" {
			s.RedpandaBrokers = strings.Split(brokers, ",")
		} else {
			s.RedpandaBrokers = []string{"localhost:9092"}
		}
	}
	if s.RawEventsTopic == "" {
		s.RawEventsTopic = "events.raw"
	}
	if s.DLQTopic == "" {
		s.DLQTopic = "events.dlq"
	}
	if s.AlertsTopic == "" {
		s.AlertsTopic = "events.alerts"
	}
	if s.ClickHouseDSN == "" {
		s.ClickHouseDSN = getEnv("CLICKHOUSE_DSN", "clickhouse://default:pulse_secret@localhost:9000/pulse")
	}
	if s.RedisURL == "" {
		s.RedisURL = getEnv("REDIS_URL", "redis://localhost:6379/0")
	}
	if s.IngestMaxFutureSkewSec == 0 {
		s.IngestMaxFutureSkewSec = 60
	}
	if s.IngestMaxPastSkewSec == 0 {
		s.IngestMaxPastSkewSec = 3600
	}
	if s.MetricsDir == "" {
		s.MetricsDir = "configs/metrics"
	}
	if s.FlushBatchSize == 0 {
		s.FlushBatchSize = 1000
	}
	if s.FlushIntervalMs == 0 {
		s.FlushIntervalMs = 500
	}
}

func applyMetricDefaults(m *MetricRule) {
	if len(m.Window.TumblingSizes) == 0 {
		m.Window.TumblingSizes = []string{"1m"}
	}
	if m.Window.AllowedLatenessSec == 0 {
		m.Window.AllowedLatenessSec = 30
	}
	if m.Window.DedupTTLSec == 0 {
		m.Window.DedupTTLSec = 300
	}
	if m.Baselines.SeasonalType == "" {
		m.Baselines.SeasonalType = "hour_of_week"
	}
	if m.Baselines.MinObservations == 0 {
		m.Baselines.MinObservations = 10
	}
	if m.Detectors.Spike.Algorithm == "" {
		m.Detectors.Spike.Algorithm = "ewma_zscore"
	}
	if m.Detectors.Spike.Alpha == 0 {
		m.Detectors.Spike.Alpha = 0.25
	}
	if m.Detectors.Spike.ZThreshold == 0 {
		m.Detectors.Spike.ZThreshold = 3.0
	}
	if m.Detectors.Drift.Algorithm == "" {
		m.Detectors.Drift.Algorithm = "page_hinkley"
	}
	if m.Detectors.Drift.Delta == 0 {
		m.Detectors.Drift.Delta = 0.1
	}
	if m.Detectors.Drift.Threshold == 0 {
		m.Detectors.Drift.Threshold = 15.0
	}
	if m.Detectors.Drift.Alpha == 0 {
		m.Detectors.Drift.Alpha = 0.99
	}
	if m.Detectors.Silence.HeartbeatTimeoutSec == 0 {
		m.Detectors.Silence.HeartbeatTimeoutSec = 120
	}
	if m.Alerting.Severity == "" {
		m.Alerting.Severity = "warning"
	}
	if m.Alerting.CooldownSec == 0 {
		m.Alerting.CooldownSec = 300
	}
	if m.Forecasting.DefaultAlgorithm == "" {
		m.Forecasting.DefaultAlgorithm = "holt_winters"
	}
	if m.Forecasting.SeasonalityPeriod == 0 {
		m.Forecasting.SeasonalityPeriod = 168
	}
}

func (c *Config) Validate() error {
	if len(c.Metrics) == 0 {
		return ErrNoMetricsFound
	}

	for name, m := range c.Metrics {
		if len(m.Window.TumblingSizes) == 0 {
			return fmt.Errorf("metric %q: %w", name, ErrInvalidWindowSize)
		}
		for _, size := range m.Window.TumblingSizes {
			if _, err := ParseDuration(size); err != nil {
				return fmt.Errorf("metric %q invalid window size %q: %w", name, size, err)
			}
		}
		if m.Window.AllowedLatenessSec < 0 {
			return fmt.Errorf("metric %q: %w", name, ErrInvalidLateness)
		}
		if m.Window.DedupTTLSec <= 0 {
			return fmt.Errorf("metric %q: %w", name, ErrInvalidDedupTTL)
		}
		if m.Detectors.Spike.Enabled {
			if m.Detectors.Spike.Alpha <= 0 || m.Detectors.Spike.Alpha > 1.0 {
				return fmt.Errorf("metric %q: %w", name, ErrInvalidAlpha)
			}
			if m.Detectors.Spike.ZThreshold <= 0 {
				return fmt.Errorf("metric %q: %w", name, ErrInvalidZThreshold)
			}
		}
	}
	return nil
}

// ParseDuration parses strings like "1m", "5m", "1h", "1d" into time.Duration.
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "d") {
		daysStr := strings.TrimSuffix(s, "d")
		var days int
		if _, err := fmt.Sscanf(daysStr, "%d", &days); err != nil {
			return 0, fmt.Errorf("invalid day duration: %s", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func getEnv(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}
