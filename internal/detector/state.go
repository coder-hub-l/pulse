package detector

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// StateStore persists and restores detector state (baselines, EWMA, PH accumulators) to Redis.
type StateStore struct {
	client *redis.Client
	prefix string
}

func NewStateStore(client *redis.Client, prefix string) *StateStore {
	if prefix == "" {
		prefix = "pulse:state"
	}
	return &StateStore{client: client, prefix: prefix}
}

// SaveBaselines persists the 168-slot seasonal baselines for a metric key to Redis.
func (s *StateStore) SaveBaselines(ctx context.Context, metricKey string, baselines *SeasonalBaselines) error {
	data, err := json.Marshal(baselines)
	if err != nil {
		return fmt.Errorf("failed to marshal baselines for %q: %w", metricKey, err)
	}
	key := fmt.Sprintf("%s:baselines:%s", s.prefix, metricKey)
	return s.client.Set(ctx, key, data, 30*24*time.Hour).Err()
}

// LoadBaselines restores seasonal baselines from Redis. Returns nil if not found.
func (s *StateStore) LoadBaselines(ctx context.Context, metricKey string) (*SeasonalBaselines, error) {
	key := fmt.Sprintf("%s:baselines:%s", s.prefix, metricKey)
	data, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load baselines for %q: %w", metricKey, err)
	}

	var baselines SeasonalBaselines
	if err := json.Unmarshal(data, &baselines); err != nil {
		return nil, fmt.Errorf("failed to unmarshal baselines for %q: %w", metricKey, err)
	}
	return &baselines, nil
}

// SaveSnapshot persists arbitrary JSON-serializable detector state keyed by detector+metric.
func (s *StateStore) SaveSnapshot(ctx context.Context, detectorName, metricKey string, state interface{}) error {
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to marshal state for %s/%s: %w", detectorName, metricKey, err)
	}
	key := fmt.Sprintf("%s:snapshot:%s:%s", s.prefix, detectorName, metricKey)
	return s.client.Set(ctx, key, data, 30*24*time.Hour).Err()
}

// LoadSnapshot restores detector state from Redis. Caller provides a pointer to unmarshal into.
func (s *StateStore) LoadSnapshot(ctx context.Context, detectorName, metricKey string, dest interface{}) error {
	key := fmt.Sprintf("%s:snapshot:%s:%s", s.prefix, detectorName, metricKey)
	data, err := s.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return fmt.Errorf("failed to load snapshot for %s/%s: %w", detectorName, metricKey, err)
	}
	return json.Unmarshal(data, dest)
}

// MemoryStateStore provides an in-memory state store for unit testing.
type MemoryStateStore struct {
	data map[string][]byte
}

func NewMemoryStateStore() *MemoryStateStore {
	return &MemoryStateStore{data: make(map[string][]byte)}
}

func (m *MemoryStateStore) SaveBaselines(ctx context.Context, metricKey string, baselines *SeasonalBaselines) error {
	data, err := json.Marshal(baselines)
	if err != nil {
		return err
	}
	m.data["baselines:"+metricKey] = data
	return nil
}

func (m *MemoryStateStore) LoadBaselines(ctx context.Context, metricKey string) (*SeasonalBaselines, error) {
	data, exists := m.data["baselines:"+metricKey]
	if !exists {
		return nil, nil
	}
	var bl SeasonalBaselines
	return &bl, json.Unmarshal(data, &bl)
}
