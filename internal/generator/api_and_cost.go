package generator

import (
	"math/rand"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// GenerateAPIEvents produces API latency percentiles and error rates across endpoints
func GenerateAPIEvents(endpoints []string, count int) ([]domain.Event, *GroundTruthLog) {
	if len(endpoints) == 0 {
		endpoints = []string{"/checkout", "/cart", "/search", "/login"}
	}
	gt := &GroundTruthLog{}
	var events []domain.Event
	now := time.Now().UTC()

	rng := rand.New(rand.NewSource(99))

	for i := 0; i < count; i++ {
		ep := endpoints[i%len(endpoints)]
		ts := now.Add(-time.Duration(count-i) * 500 * time.Millisecond)

		baseLatency := 25.0 + rng.Float64()*15.0
		eventID := uuid.New().String()

		// Inject latency spike on /checkout
		if ep == "/checkout" && i == count/2 {
			injected := 850.0
			gt.Labels = append(gt.Labels, InjectedAnomalyLabel{
				EventID:       eventID,
				MetricKey:     "api.latency_p99",
				Timestamp:     ts,
				AnomalyType:   domain.AnomalyTypeSpike,
				InjectedValue: injected,
				BaselineValue: baseLatency,
				Reason:        "Downstream payment gateway outage latency spike",
			})
			baseLatency = injected
		}

		events = append(events, domain.Event{
			EventID:   eventID,
			MetricKey: "api.latency_p99",
			Timestamp: ts,
			Value:     baseLatency,
			Dimensions: map[string]string{
				"endpoint": ep,
				"service":  "api-gateway",
			},
		})
	}
	return events, gt
}

// GenerateCloudCostEvents produces daily infrastructure spend events per service
func GenerateCloudCostEvents(days int) ([]domain.Event, *GroundTruthLog) {
	services := []string{"compute-k8s", "clickhouse-cluster", "redpanda-broker", "redis-cache"}
	gt := &GroundTruthLog{}
	var events []domain.Event
	start := time.Now().UTC().AddDate(0, 0, -days)

	rng := rand.New(rand.NewSource(123))

	for d := 0; d < days; d++ {
		ts := start.AddDate(0, 0, d)
		for _, svc := range services {
			baseCost := 150.0 + rng.Float64()*30.0
			eventID := uuid.New().String()

			// Inject drift on compute-k8s: runaway memory leak leading to autoscale cost creep
			if svc == "compute-k8s" && d > days/2 {
				drift := float64(d-days/2) * 25.0
				injected := baseCost + drift
				gt.Labels = append(gt.Labels, InjectedAnomalyLabel{
					EventID:       eventID,
					MetricKey:     "cloud.daily_spend",
					Timestamp:     ts,
					AnomalyType:   domain.AnomalyTypeDrift,
					InjectedValue: injected,
					BaselineValue: baseCost,
					Reason:        "Autoscaler scale-out cost drift",
				})
				baseCost = injected
			}

			events = append(events, domain.Event{
				EventID:   eventID,
				MetricKey: "cloud.daily_spend",
				Timestamp: ts,
				Value:     baseCost,
				Dimensions: map[string]string{
					"service":     svc,
					"environment": "production",
				},
			})
		}
	}
	return events, gt
}
