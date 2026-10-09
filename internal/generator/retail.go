package generator

import (
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// InjectedAnomalyLabel documents ground truth for benchmark precision/recall evaluation.
type InjectedAnomalyLabel struct {
	EventID       string               `json:"event_id"`
	MetricKey     string               `json:"metric_key"`
	Timestamp     time.Time            `json:"timestamp"`
	AnomalyType   domain.AnomalyType   `json:"anomaly_type"`
	InjectedValue float64              `json:"injected_value"`
	BaselineValue float64              `json:"baseline_value"`
	Reason        string               `json:"reason"`
}

// GroundTruthLog stores labels for offline evaluation.
type GroundTruthLog struct {
	Labels []InjectedAnomalyLabel `json:"labels"`
}

func (g *GroundTruthLog) SaveToFile(filepath string) error {
	data, err := json.MarshalIndent(g, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath, data, 0644)
}

// GenerateRetailEvents generates realistic multi-store retail data with:
// - 50+ stores
// - Lunch (12-14) & dinner (19-21) peaks
// - Weekend / festival effects
// - Injected anomalies: spike, drift, silence, duplicates, late events
func GenerateRetailEvents(numStores int, duration time.Duration, step time.Duration, groundTruthPath string) ([]domain.Event, *GroundTruthLog) {
	if numStores <= 0 {
		numStores = 50
	}
	if step <= 0 {
		step = time.Minute
	}

	gt := &GroundTruthLog{}
	var events []domain.Event

	startTime := time.Now().UTC().Add(-duration)
	totalSteps := int(duration / step)

	rng := rand.New(rand.NewSource(42)) // Deterministic seed

	for s := 1; s <= numStores; s++ {
		storeID := fmt.Sprintf("store_%03d", s)
		region := "north"
		if s > 35 {
			region = "west"
		} else if s > 20 {
			region = "south"
		}

		baseSales := 50.0 + rng.Float64()*50.0 // Baseline volume per store

		for stepIdx := 0; stepIdx < totalSteps; stepIdx++ {
			ts := startTime.Add(time.Duration(stepIdx) * step)
			hour := ts.Hour()

			// Daily diurnal shape: lunch peak (13h) and dinner peak (20h)
			diurnalMultiplier := 0.3
			if hour >= 11 && hour <= 14 {
				diurnalMultiplier = 1.8 // Lunch rush
			} else if hour >= 18 && hour <= 21 {
				diurnalMultiplier = 2.2 // Dinner peak
			} else if hour >= 8 && hour <= 22 {
				diurnalMultiplier = 1.0 // Normal shopping hours
			}

			// Weekend boost (Saturday=6, Sunday=0)
			weekendMultiplier := 1.0
			if ts.Weekday() == time.Saturday || ts.Weekday() == time.Sunday {
				weekendMultiplier = 1.4
			}

			val := baseSales * diurnalMultiplier * weekendMultiplier * (0.9 + rng.Float64()*0.2)
			eventID := uuid.New().String()

			// 1. Inject Spike: Store 5 around step 100 has a massive promotion/flash-sale
			if s == 5 && stepIdx == 100 {
				injected := val * 5.0
				gt.Labels = append(gt.Labels, InjectedAnomalyLabel{
					EventID:       eventID,
					MetricKey:     "retail.orders",
					Timestamp:     ts,
					AnomalyType:   domain.AnomalyTypeSpike,
					InjectedValue: injected,
					BaselineValue: val,
					Reason:        "Flash sale sudden spike",
				})
				val = injected
			}

			// 2. Inject Gradual Drift: Store 12 experiences linear upward drift from step 80 onwards
			if s == 12 && stepIdx >= 80 && stepIdx <= 150 {
				drift := float64(stepIdx-80) * 1.5
				injected := val + drift
				gt.Labels = append(gt.Labels, InjectedAnomalyLabel{
					EventID:       eventID,
					MetricKey:     "retail.orders",
					Timestamp:     ts,
					AnomalyType:   domain.AnomalyTypeDrift,
					InjectedValue: injected,
					BaselineValue: val,
					Reason:        "Gradual footfall drift upward",
				})
				val = injected
			}

			// 3. Inject Silence: Store 25 has network outage between steps 50..70 (omit emitting events)
			if s == 25 && stepIdx >= 50 && stepIdx <= 70 {
				if stepIdx == 50 {
					gt.Labels = append(gt.Labels, InjectedAnomalyLabel{
						EventID:       "",
						MetricKey:     "retail.orders",
						Timestamp:     ts,
						AnomalyType:   domain.AnomalyTypeSilence,
						InjectedValue: 0,
						BaselineValue: val,
						Reason:        "Store POS network blackout (silence)",
					})
				}
				continue // Skip emitting
			}

			ev := domain.Event{
				EventID:   eventID,
				MetricKey: "retail.orders",
				Timestamp: ts,
				Value:     math.Round(val*100) / 100,
				Dimensions: map[string]string{
					"store_id": storeID,
					"region":   region,
				},
			}
			events = append(events, ev)

			// 4. Inject Duplicate: Store 3 duplicate emit at step 40
			if s == 3 && stepIdx == 40 {
				events = append(events, ev) // Identical EventID
			}

			// 5. Inject Out-of-order / Late event: Store 7 emits a late event at step 90
			if s == 7 && stepIdx == 90 {
				lateEv := domain.Event{
					EventID:   uuid.New().String(),
					MetricKey: "retail.orders",
					Timestamp: ts.Add(-2 * time.Hour), // 2 hours late!
					Value:     val,
					Dimensions: map[string]string{
						"store_id": storeID,
						"region":   region,
					},
				}
				events = append(events, lateEv)
			}
		}
	}

	if groundTruthPath != "" {
		_ = gt.SaveToFile(groundTruthPath)
	}

	return events, gt
}
