package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

const validYAML = `
server:
  port: 8080
  read_timeout: "5s"
  write_timeout: "10s"

postgres:
  dsn: "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"

redis:
  addr: "localhost:6379"
  db: 0

defaults:
  window_size: "60s"
  allowed_lateness: "30s"
  dedup_ttl: "300s"
  ewma_alpha: 0.3
  z_score_threshold: 3.0
  heartbeat_timeout: "180s"
  alert_cooldown: "300s"

metrics:
  "api.latency_p99":
    window_size: "30s"
    z_score_threshold: 2.5
    heartbeat_timeout: "60s"
  "orders.count":
    ewma_alpha: 0.1
    alert_cooldown: "600s"
`

func writeTestConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidConfig(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if cfg.Server.Port != 8080 {
		t.Errorf("port: got %d", cfg.Server.Port)
	}
	if cfg.Server.ReadTimeout.Duration != 5*time.Second {
		t.Errorf("read_timeout: got %v", cfg.Server.ReadTimeout.Duration)
	}
	if cfg.Postgres.DSN == "" {
		t.Error("postgres.dsn should not be empty")
	}
	if cfg.Redis.Addr != "localhost:6379" {
		t.Errorf("redis.addr: got %q", cfg.Redis.Addr)
	}
	if cfg.Defaults.EWMAAlpha != 0.3 {
		t.Errorf("defaults.ewma_alpha: got %f", cfg.Defaults.EWMAAlpha)
	}
}

func TestForMetricDefaults(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	// Unknown metric gets pure defaults.
	s := cfg.ForMetric("unknown.metric")
	if s.WindowSize != 60*time.Second {
		t.Errorf("window_size: got %v", s.WindowSize)
	}
	if s.EWMAAlpha != 0.3 {
		t.Errorf("ewma_alpha: got %f", s.EWMAAlpha)
	}
	if s.ZScoreThreshold != 3.0 {
		t.Errorf("z_score_threshold: got %f", s.ZScoreThreshold)
	}
}

func TestForMetricOverride(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	s := cfg.ForMetric("api.latency_p99")

	// Overridden fields.
	if s.WindowSize != 30*time.Second {
		t.Errorf("window_size: expected 30s, got %v", s.WindowSize)
	}
	if s.ZScoreThreshold != 2.5 {
		t.Errorf("z_score_threshold: expected 2.5, got %f", s.ZScoreThreshold)
	}
	if s.HeartbeatTimeout != 60*time.Second {
		t.Errorf("heartbeat_timeout: expected 60s, got %v", s.HeartbeatTimeout)
	}

	// Non-overridden fields fall back to defaults.
	if s.EWMAAlpha != 0.3 {
		t.Errorf("ewma_alpha should be default 0.3, got %f", s.EWMAAlpha)
	}
	if s.DedupTTL != 300*time.Second {
		t.Errorf("dedup_ttl should be default 300s, got %v", s.DedupTTL)
	}
	if s.AlertCooldown != 300*time.Second {
		t.Errorf("alert_cooldown should be default 300s, got %v", s.AlertCooldown)
	}
}

func TestForMetricSecondOverride(t *testing.T) {
	path := writeTestConfig(t, validYAML)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	s := cfg.ForMetric("orders.count")
	if s.EWMAAlpha != 0.1 {
		t.Errorf("ewma_alpha: expected 0.1, got %f", s.EWMAAlpha)
	}
	if s.AlertCooldown != 600*time.Second {
		t.Errorf("alert_cooldown: expected 600s, got %v", s.AlertCooldown)
	}
	// Everything else should be defaults.
	if s.WindowSize != 60*time.Second {
		t.Errorf("window_size should be default 60s, got %v", s.WindowSize)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	path := writeTestConfig(t, "{{{{not yaml")
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid YAML")
	}
}

func TestLoadInvalidPort(t *testing.T) {
	bad := validYAML
	// Replace port with 0.
	path := writeTestConfig(t, replaceInYAML(bad, "port: 8080", "port: 0"))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for port=0")
	}
}

func TestLoadInvalidEWMAAlpha(t *testing.T) {
	path := writeTestConfig(t, replaceInYAML(validYAML, "ewma_alpha: 0.3", "ewma_alpha: 1.5"))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for ewma_alpha > 1")
	}
}

func TestLoadZeroEWMAAlpha(t *testing.T) {
	path := writeTestConfig(t, replaceInYAML(validYAML, "ewma_alpha: 0.3", "ewma_alpha: 0.0"))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for ewma_alpha = 0")
	}
}

func TestLoadEmptyDSN(t *testing.T) {
	path := writeTestConfig(t, replaceInYAML(validYAML,
		`dsn: "postgres://pulse:pulse@localhost:5432/pulse?sslmode=disable"`,
		`dsn: ""`))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty DSN")
	}
}

func TestLoadNegativeZScore(t *testing.T) {
	path := writeTestConfig(t, replaceInYAML(validYAML,
		"z_score_threshold: 3.0", "z_score_threshold: -1.0"))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for negative z_score_threshold")
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	path := writeTestConfig(t, replaceInYAML(validYAML,
		`window_size: "60s"`, `window_size: "not_a_duration"`))
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid duration string")
	}
}

// replaceInYAML is a test helper that does simple string replacement.
func replaceInYAML(yaml, old, new string) string {
	return stringReplace(yaml, old, new, 1)
}

func stringReplace(s, old, new string, n int) string {
	i := 0
	for count := 0; count < n; count++ {
		j := indexOf(s[i:], old)
		if j < 0 {
			break
		}
		s = s[:i+j] + new + s[i+j+len(old):]
		i = i + j + len(new)
	}
	return s
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
