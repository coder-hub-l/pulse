package domain

import (
	"time"
)

type AnomalyType string

const (
	AnomalyTypeSpike   AnomalyType = "spike"
	AnomalyTypeDip     AnomalyType = "dip"
	AnomalyTypeDrift   AnomalyType = "drift"
	AnomalyTypeSilence AnomalyType = "silence"
)

type Severity string

const (
	SeverityInfo     Severity = "info"
	SeverityWarning  Severity = "warning"
	SeverityCritical Severity = "critical"
)

// Anomaly represents a detected statistical or operational deviation.
type Anomaly struct {
	ID             string            `json:"id" db:"id"`
	EventID        string            `json:"event_id" db:"event_id"`
	MetricKey      string            `json:"metric_key" db:"metric_key"`
	Timestamp      time.Time         `json:"timestamp" db:"timestamp"`
	ObservedValue  float64           `json:"observed_value" db:"observed_value"`
	ExpectedMean   float64           `json:"expected_mean" db:"expected_mean"`
	ExpectedStdDev float64           `json:"expected_stddev" db:"expected_stddev"`
	Score          float64           `json:"score" db:"score"` // e.g., Z-score magnitude or drift statistic
	Type           AnomalyType       `json:"anomaly_type" db:"anomaly_type"`
	Severity       Severity          `json:"severity" db:"severity"`
	Dimensions     map[string]string `json:"dimensions,omitempty" db:"dimensions"`
	CreatedAt      time.Time         `json:"created_at" db:"created_at"`
}
