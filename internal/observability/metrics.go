package observability

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	EventsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "ingest",
			Name:      "events_total",
			Help:      "Total number of events processed by ingest API",
		},
		[]string{"metric_key", "status"},
	)

	EventsBufferLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pulse",
			Subsystem: "ingest",
			Name:      "buffer_publish_duration_seconds",
			Help:      "Latency of publishing events to Redpanda buffer",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"metric_key"},
	)

	EventsDedupedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "pipeline",
			Name:      "deduped_total",
			Help:      "Total number of duplicate events filtered",
		},
		[]string{"metric_key"},
	)

	EventsLateTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "pipeline",
			Name:      "late_events_total",
			Help:      "Total number of late events routed to DLQ or window",
		},
		[]string{"metric_key", "disposition"}, // "applied", "dlq"
	)

	AnomaliesDetectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "detector",
			Name:      "anomalies_total",
			Help:      "Total number of anomalies detected",
		},
		[]string{"metric_key", "type", "severity"},
	)

	PipelineProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "pulse",
			Subsystem: "pipeline",
			Name:      "processing_duration_seconds",
			Help:      "End-to-end streaming latency from ingestion to detection",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
		},
		[]string{"metric_key"},
	)

	AlertsFiredTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "alert",
			Name:      "alerts_fired_total",
			Help:      "Total number of alerts fired to sinks",
		},
		[]string{"metric_key", "severity"},
	)

	ConsumerLagGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "pulse",
			Subsystem: "queue",
			Name:      "consumer_lag",
			Help:      "Current consumer lag in partitions",
		},
		[]string{"topic", "partition"},
	)

	DLQEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "pulse",
			Subsystem: "pipeline",
			Name:      "dlq_events_total",
			Help:      "Total count of unrecoverable or severely late events routed to DLQ",
		},
		[]string{"metric_key", "reason"},
	)
)
