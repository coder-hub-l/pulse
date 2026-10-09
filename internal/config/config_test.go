package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoad_FullMultiFile(t *testing.T) {
	tmpDir := t.TempDir()
	metricsDir := filepath.Join(tmpDir, "metrics")
	require.NoError(t, os.MkdirAll(metricsDir, 0755))

	serverYaml := `
server:
  http_port: 8888
  grpc_port: 9999
  redpanda_brokers: ["broker1:9092", "broker2:9092"]
  clickhouse_dsn: "clickhouse://localhost:9000"
  redis_url: "redis://localhost:6379"
`
	serverPath := filepath.Join(tmpDir, "pulse.yaml")
	require.NoError(t, os.WriteFile(serverPath, []byte(serverYaml), 0644))

	retailYaml := `
version: "1.0"
namespace: "retail"
metrics:
  "retail.store.orders":
    description: "Store orders"
    window:
      tumbling_sizes: ["1m", "5m"]
      allowed_lateness_sec: 45
      dedup_ttl_sec: 300
    detectors:
      spike:
        enabled: true
        alpha: 0.3
        z_threshold: 3.5
`
	require.NoError(t, os.WriteFile(filepath.Join(metricsDir, "retail.yaml"), []byte(retailYaml), 0644))

	cfg, err := Load(serverPath, metricsDir)
	require.NoError(t, err)
	require.NotNil(t, cfg)

	assert.Equal(t, 8888, cfg.Server.HTTPPort)
	assert.Equal(t, 9999, cfg.Server.GRPCPort)
	assert.Len(t, cfg.Server.RedpandaBrokers, 2)

	m, ok := cfg.Metrics["retail.store.orders"]
	require.True(t, ok)
	assert.Equal(t, time.Minute, m.PrimaryWindowDuration())
	assert.Equal(t, 45*time.Second, m.AllowedLateness())
	assert.Equal(t, 300*time.Second, m.DedupTTL())
	assert.Equal(t, 0.3, m.Detectors.Spike.Alpha)
	assert.Equal(t, 3.5, m.Detectors.Spike.ZThreshold)
}

func TestParseDuration(t *testing.T) {
	d1, err := ParseDuration("1m")
	require.NoError(t, err)
	assert.Equal(t, time.Minute, d1)

	d2, err := ParseDuration("5m")
	require.NoError(t, err)
	assert.Equal(t, 5*time.Minute, d2)

	d3, err := ParseDuration("1h")
	require.NoError(t, err)
	assert.Equal(t, time.Hour, d3)

	d4, err := ParseDuration("2d")
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, d4)
}
