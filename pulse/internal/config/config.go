package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Duration wraps time.Duration to support YAML unmarshalling from strings
// like "60s", "5m", "1h". Go's time.Duration doesn't implement
// yaml.Unmarshaler, so raw `time.Duration` fields would only accept
// integer nanoseconds — unreadable and error-prone.
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalYAML(value *yaml.Node) error {
	var s string
	if err := value.Decode(&s); err != nil {
		return fmt.Errorf("duration must be a string: %w", err)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", s, err)
	}
	d.Duration = parsed
	return nil
}

func (d Duration) MarshalYAML() (interface{}, error) {
	return d.Duration.String(), nil
}

// ServerConfig holds HTTP server settings.
type ServerConfig struct {
	Port         int      `yaml:"port"`
	ReadTimeout  Duration `yaml:"read_timeout"`
	WriteTimeout Duration `yaml:"write_timeout"`
}

// PostgresConfig holds Postgres connection settings.
type PostgresConfig struct {
	DSN string `yaml:"dsn"`
}

// RedisConfig holds Redis connection settings.
type RedisConfig struct {
	Addr string `yaml:"addr"`
	DB   int    `yaml:"db"`
}

// MetricSettings holds the resolved (fully-merged) per-metric configuration.
// Every field has a concrete value — no pointers, no nils.
type MetricSettings struct {
	WindowSize       time.Duration
	AllowedLateness  time.Duration
	DedupTTL         time.Duration
	EWMAAlpha        float64
	ZScoreThreshold  float64
	HeartbeatTimeout time.Duration
	AlertCooldown    time.Duration
}

// metricOverride holds optional per-metric overrides in YAML. Pointer fields
// distinguish "not set" from "set to zero". This is the standard Go pattern
// for partial/patch updates — interviewers love asking about this.
type metricOverride struct {
	WindowSize       *Duration `yaml:"window_size"`
	AllowedLateness  *Duration `yaml:"allowed_lateness"`
	DedupTTL         *Duration `yaml:"dedup_ttl"`
	EWMAAlpha        *float64  `yaml:"ewma_alpha"`
	ZScoreThreshold  *float64  `yaml:"z_score_threshold"`
	HeartbeatTimeout *Duration `yaml:"heartbeat_timeout"`
	AlertCooldown    *Duration `yaml:"alert_cooldown"`
}

// defaultsBlock mirrors metricOverride but all fields are required.
type defaultsBlock struct {
	WindowSize       Duration `yaml:"window_size"`
	AllowedLateness  Duration `yaml:"allowed_lateness"`
	DedupTTL         Duration `yaml:"dedup_ttl"`
	EWMAAlpha        float64  `yaml:"ewma_alpha"`
	ZScoreThreshold  float64  `yaml:"z_score_threshold"`
	HeartbeatTimeout Duration `yaml:"heartbeat_timeout"`
	AlertCooldown    Duration `yaml:"alert_cooldown"`
}

// Config is the top-level application configuration loaded from YAML.
type Config struct {
	Server   ServerConfig              `yaml:"server"`
	Postgres PostgresConfig            `yaml:"postgres"`
	Redis    RedisConfig               `yaml:"redis"`
	Defaults defaultsBlock             `yaml:"defaults"`
	Metrics  map[string]metricOverride `yaml:"metrics"`
}

// Load reads and parses a YAML config file, then validates all fields.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("config: validate: %w", err)
	}

	return &cfg, nil
}

// ForMetric returns fully-resolved settings for a given metric key.
// Per-metric overrides are merged on top of defaults. Unknown metric keys
// get pure defaults — this is intentional so new metrics work without
// config changes.
func (c *Config) ForMetric(key string) MetricSettings {
	s := MetricSettings{
		WindowSize:       c.Defaults.WindowSize.Duration,
		AllowedLateness:  c.Defaults.AllowedLateness.Duration,
		DedupTTL:         c.Defaults.DedupTTL.Duration,
		EWMAAlpha:        c.Defaults.EWMAAlpha,
		ZScoreThreshold:  c.Defaults.ZScoreThreshold,
		HeartbeatTimeout: c.Defaults.HeartbeatTimeout.Duration,
		AlertCooldown:    c.Defaults.AlertCooldown.Duration,
	}

	override, ok := c.Metrics[key]
	if !ok {
		return s
	}

	// Merge: only override fields that are explicitly set (non-nil pointer).
	if override.WindowSize != nil {
		s.WindowSize = override.WindowSize.Duration
	}
	if override.AllowedLateness != nil {
		s.AllowedLateness = override.AllowedLateness.Duration
	}
	if override.DedupTTL != nil {
		s.DedupTTL = override.DedupTTL.Duration
	}
	if override.EWMAAlpha != nil {
		s.EWMAAlpha = *override.EWMAAlpha
	}
	if override.ZScoreThreshold != nil {
		s.ZScoreThreshold = *override.ZScoreThreshold
	}
	if override.HeartbeatTimeout != nil {
		s.HeartbeatTimeout = override.HeartbeatTimeout.Duration
	}
	if override.AlertCooldown != nil {
		s.AlertCooldown = override.AlertCooldown.Duration
	}

	return s
}

// validate checks that all required fields are present and within sane bounds.
func (c *Config) validate() error {
	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("server.port must be 1-65535, got %d", c.Server.Port)
	}
	if c.Postgres.DSN == "" {
		return fmt.Errorf("postgres.dsn must not be empty")
	}
	if c.Redis.Addr == "" {
		return fmt.Errorf("redis.addr must not be empty")
	}

	// Validate defaults.
	if c.Defaults.WindowSize.Duration <= 0 {
		return fmt.Errorf("defaults.window_size must be positive")
	}
	if c.Defaults.AllowedLateness.Duration < 0 {
		return fmt.Errorf("defaults.allowed_lateness must be non-negative")
	}
	if c.Defaults.DedupTTL.Duration <= 0 {
		return fmt.Errorf("defaults.dedup_ttl must be positive")
	}
	if c.Defaults.EWMAAlpha <= 0 || c.Defaults.EWMAAlpha > 1 {
		return fmt.Errorf("defaults.ewma_alpha must be in (0, 1], got %f", c.Defaults.EWMAAlpha)
	}
	if c.Defaults.ZScoreThreshold <= 0 {
		return fmt.Errorf("defaults.z_score_threshold must be positive, got %f", c.Defaults.ZScoreThreshold)
	}
	if c.Defaults.HeartbeatTimeout.Duration <= 0 {
		return fmt.Errorf("defaults.heartbeat_timeout must be positive")
	}
	if c.Defaults.AlertCooldown.Duration <= 0 {
		return fmt.Errorf("defaults.alert_cooldown must be positive")
	}

	// Validate per-metric overrides (only check set fields).
	for key, m := range c.Metrics {
		if m.EWMAAlpha != nil && (*m.EWMAAlpha <= 0 || *m.EWMAAlpha > 1) {
			return fmt.Errorf("metrics[%q].ewma_alpha must be in (0, 1], got %f", key, *m.EWMAAlpha)
		}
		if m.ZScoreThreshold != nil && *m.ZScoreThreshold <= 0 {
			return fmt.Errorf("metrics[%q].z_score_threshold must be positive, got %f", key, *m.ZScoreThreshold)
		}
		if m.WindowSize != nil && m.WindowSize.Duration <= 0 {
			return fmt.Errorf("metrics[%q].window_size must be positive", key)
		}
		if m.AllowedLateness != nil && m.AllowedLateness.Duration < 0 {
			return fmt.Errorf("metrics[%q].allowed_lateness must be non-negative", key)
		}
		if m.DedupTTL != nil && m.DedupTTL.Duration <= 0 {
			return fmt.Errorf("metrics[%q].dedup_ttl must be positive", key)
		}
		if m.HeartbeatTimeout != nil && m.HeartbeatTimeout.Duration <= 0 {
			return fmt.Errorf("metrics[%q].heartbeat_timeout must be positive", key)
		}
		if m.AlertCooldown != nil && m.AlertCooldown.Duration <= 0 {
			return fmt.Errorf("metrics[%q].alert_cooldown must be positive", key)
		}
	}

	return nil
}
