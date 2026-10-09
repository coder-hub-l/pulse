package alert

import (
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEngine_BasicAlertFiring(t *testing.T) {
	sink := NewMemorySink()
	engine := NewEngine(5*time.Minute, []string{"metric_key"}, sink)

	anomalies := []domain.Anomaly{
		{
			ID:            "a-1",
			MetricKey:     "retail.orders",
			Timestamp:     time.Now().UTC(),
			ObservedValue: 500.0,
			ExpectedMean:  100.0,
			ExpectedStdDev: 15.0,
			Score:         26.6,
			Type:          domain.AnomalyTypeSpike,
			Severity:      domain.SeverityCritical,
		},
	}

	fired := engine.Process(anomalies)
	require.Len(t, fired, 1)
	assert.Equal(t, domain.AlertStatusFiring, fired[0].Status)
	assert.Equal(t, domain.SeverityCritical, fired[0].Severity)
	assert.Len(t, sink.Alerts, 1)
}

func TestEngine_CooldownSuppression(t *testing.T) {
	sink := NewMemorySink()
	engine := NewEngine(5*time.Minute, []string{"metric_key"}, sink)

	anomaly := domain.Anomaly{
		ID:        "a-1",
		MetricKey: "retail.orders",
		Timestamp: time.Now().UTC(),
		Score:     5.0,
		Type:      domain.AnomalyTypeSpike,
		Severity:  domain.SeverityWarning,
	}

	// First fire: should send
	fired := engine.Process([]domain.Anomaly{anomaly})
	require.Len(t, fired, 1)

	// Second fire within cooldown: should suppress
	anomaly.ID = "a-2"
	fired = engine.Process([]domain.Anomaly{anomaly})
	assert.Empty(t, fired, "Alert with same fingerprint within cooldown must be suppressed")

	// Only 1 alert should have been dispatched to the sink
	assert.Len(t, sink.Alerts, 1)
}

func TestEngine_DifferentFingerprintsFireSeparately(t *testing.T) {
	sink := NewMemorySink()
	engine := NewEngine(5*time.Minute, []string{"metric_key", "store_id"}, sink)

	anomalies := []domain.Anomaly{
		{
			ID:         "a-1",
			MetricKey:  "retail.orders",
			Type:       domain.AnomalyTypeSpike,
			Severity:   domain.SeverityCritical,
			Dimensions: map[string]string{"store_id": "store_A"},
		},
		{
			ID:         "a-2",
			MetricKey:  "retail.orders",
			Type:       domain.AnomalyTypeSpike,
			Severity:   domain.SeverityWarning,
			Dimensions: map[string]string{"store_id": "store_B"},
		},
	}

	fired := engine.Process(anomalies)
	assert.Len(t, fired, 2, "Different dimension groups must fire separate alerts")
	assert.Len(t, sink.Alerts, 2)
}

func TestEngine_GroupedAnomaliesPickHighestSeverity(t *testing.T) {
	sink := NewMemorySink()
	engine := NewEngine(5*time.Minute, []string{"metric_key"}, sink)

	anomalies := []domain.Anomaly{
		{
			ID:        "a-low",
			MetricKey: "orders",
			Type:      domain.AnomalyTypeSpike,
			Severity:  domain.SeverityInfo,
		},
		{
			ID:        "a-high",
			MetricKey: "orders",
			Type:      domain.AnomalyTypeSpike,
			Severity:  domain.SeverityCritical,
		},
	}

	fired := engine.Process(anomalies)
	require.Len(t, fired, 1)
	assert.Equal(t, domain.SeverityCritical, fired[0].Severity, "Grouped alert must use the highest severity")
}

func TestEngine_SilenceAndSpikeAreDifferentFingerprints(t *testing.T) {
	sink := NewMemorySink()
	engine := NewEngine(5*time.Minute, []string{"metric_key"}, sink)

	anomalies := []domain.Anomaly{
		{
			ID:        "a-spike",
			MetricKey: "orders",
			Type:      domain.AnomalyTypeSpike,
			Severity:  domain.SeverityWarning,
		},
		{
			ID:        "a-silence",
			MetricKey: "orders",
			Type:      domain.AnomalyTypeSilence,
			Severity:  domain.SeverityCritical,
		},
	}

	fired := engine.Process(anomalies)
	assert.Len(t, fired, 2, "Spike and silence on the same metric must produce separate alerts")
}
