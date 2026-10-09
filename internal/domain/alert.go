package domain

import (
	"time"
)

type AlertStatus string

const (
	AlertStatusFiring   AlertStatus = "firing"
	AlertStatusResolved AlertStatus = "resolved"
)

// Alert represents an aggregated notification payload dispatched to external sinks.
type Alert struct {
	ID          string            `json:"id"`
	Fingerprint string            `json:"fingerprint"` // Hash of (metric_key + group dimensions) for dedup
	MetricKey   string            `json:"metric_key"`
	Title       string            `json:"title"`
	Description string            `json:"description"`
	Severity    Severity          `json:"severity"`
	Status      AlertStatus       `json:"status"`
	Anomalies   []Anomaly         `json:"anomalies"`
	Dimensions  map[string]string `json:"dimensions,omitempty"`
	FiredAt     time.Time         `json:"fired_at"`
	ResolvedAt  *time.Time        `json:"resolved_at,omitempty"`
}
