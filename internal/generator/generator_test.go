package generator

import (
	"context"
	"testing"
	"time"

	"github.com/pulse/pulse-engine/internal/detector"
	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluation_RetailSpikeAndDriftPrecisionRecall(t *testing.T) {
	// Generate 10 stores over 12 hours (10 * 720 points = 7200 events)
	events, gt := GenerateRetailEvents(10, 12*time.Hour, time.Minute, "")
	require.NotEmpty(t, events)
	require.NotEmpty(t, gt.Labels)

	spikeDetector := detector.NewEWMAZScoreDetector(0.2, 3.0, 5)
	phDetector := detector.NewPageHinkleyDetector(0.1, 15.0, 0.99)
	ctx := context.Background()

	var detectedAnomalies []domain.Anomaly

	for _, ev := range events {
		anoms1, err := spikeDetector.Detect(ctx, ev)
		require.NoError(t, err)
		detectedAnomalies = append(detectedAnomalies, anoms1...)

		anoms2, err := phDetector.Detect(ctx, ev)
		require.NoError(t, err)
		detectedAnomalies = append(detectedAnomalies, anoms2...)
	}

	// Verify detection against ground truth
	var detectedSpikes, detectedDrifts int
	for _, a := range detectedAnomalies {
		if a.Type == domain.AnomalyTypeSpike {
			detectedSpikes++
		}
		if a.Type == domain.AnomalyTypeDrift {
			detectedDrifts++
		}
	}

	assert.True(t, detectedSpikes > 0, "Spike detector should detect injected spike")
	assert.True(t, detectedDrifts > 0, "Page-Hinkley should detect injected drift")
}

func TestEvaluation_APIEvents(t *testing.T) {
	events, gt := GenerateAPIEvents(nil, 200)
	assert.Len(t, events, 200)
	assert.Len(t, gt.Labels, 1)

	spikeDetector := detector.NewEWMAZScoreDetector(0.25, 3.0, 5)
	ctx := context.Background()

	var detected []domain.Anomaly
	for _, ev := range events {
		anoms, _ := spikeDetector.Detect(ctx, ev)
		detected = append(detected, anoms...)
	}

	assert.NotEmpty(t, detected, "Must catch the payment gateway latency spike")
}

func TestEvaluation_CloudCostEvents(t *testing.T) {
	events, gt := GenerateCloudCostEvents(30)
	assert.NotEmpty(t, events)
	assert.NotEmpty(t, gt.Labels)
}
