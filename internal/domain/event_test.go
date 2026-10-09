package domain

import (
	"testing"
	"time"
)

func TestEvent_Validate(t *testing.T) {
	now := time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
	futureSkew := 60 * time.Second
	pastSkew := 3600 * time.Second

	tests := []struct {
		name      string
		event     Event
		wantErr   error
		now       time.Time
		futSkew   time.Duration
		pastSkew  time.Duration
	}{
		{
			name: "Valid Event",
			event: Event{
				EventID:   "evt-123",
				MetricKey: "orders.placed",
				Timestamp: now.Add(-10 * time.Second),
				Value:     1.0,
			},
			wantErr:  nil,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
		{
			name: "Empty EventID",
			event: Event{
				EventID:   "   ",
				MetricKey: "orders.placed",
				Timestamp: now,
				Value:     1.0,
			},
			wantErr:  ErrEmptyEventID,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
		{
			name: "Empty MetricKey",
			event: Event{
				EventID:   "evt-123",
				MetricKey: "",
				Timestamp: now,
				Value:     1.0,
			},
			wantErr:  ErrEmptyMetricKey,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
		{
			name: "Zero Timestamp",
			event: Event{
				EventID:   "evt-123",
				MetricKey: "orders.placed",
				Timestamp: time.Time{},
				Value:     1.0,
			},
			wantErr:  ErrZeroTimestamp,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
		{
			name: "Future Timestamp Exceeding Skew",
			event: Event{
				EventID:   "evt-123",
				MetricKey: "orders.placed",
				Timestamp: now.Add(90 * time.Second),
				Value:     1.0,
			},
			wantErr:  ErrFutureTimestamp,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
		{
			name: "Past Timestamp Exceeding Skew",
			event: Event{
				EventID:   "evt-123",
				MetricKey: "orders.placed",
				Timestamp: now.Add(-4000 * time.Second),
				Value:     1.0,
			},
			wantErr:  ErrExpiredTimestamp,
			now:      now,
			futSkew:  futureSkew,
			pastSkew: pastSkew,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.event.Validate(tt.now, tt.futSkew, tt.pastSkew)
			if err != tt.wantErr {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}
