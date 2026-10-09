package dedup

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduplicator checks and records event idempotency.
type Deduplicator interface {
	// IsDuplicate returns true if the event has already been processed within the TTL.
	// Returns false and marks the event as processed if it is new.
	IsDuplicate(ctx context.Context, metricKey, eventID string, ttl time.Duration) (bool, error)
	Close() error
}

// RedisDeduplicator implements distributed deduplication using Redis SETNX with TTL.
type RedisDeduplicator struct {
	client *redis.Client
	prefix string
}

func NewRedisDeduplicator(client *redis.Client, prefix string) *RedisDeduplicator {
	if prefix == "" {
		prefix = "pulse:dedup"
	}
	return &RedisDeduplicator{
		client: client,
		prefix: prefix,
	}
}

func (d *RedisDeduplicator) IsDuplicate(ctx context.Context, metricKey, eventID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	key := fmt.Sprintf("%s:%s:%s", d.prefix, metricKey, eventID)

	// SetNX returns true if the key was set (i.e. first time seen)
	isNew, err := d.client.SetNX(ctx, key, "1", ttl).Result()
	if err != nil {
		return false, fmt.Errorf("redis dedup failed for key %q: %w", key, err)
	}

	return !isNew, nil
}

func (d *RedisDeduplicator) Close() error {
	return nil
}

// MemoryDeduplicator provides a concurrent in-memory deduplicator with TTL cleanup for testing/embedded use.
type MemoryDeduplicator struct {
	mu      sync.RWMutex
	records map[string]time.Time
}

func NewMemoryDeduplicator() *MemoryDeduplicator {
	return &MemoryDeduplicator{
		records: make(map[string]time.Time),
	}
}

func (m *MemoryDeduplicator) IsDuplicate(ctx context.Context, metricKey, eventID string, ttl time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	key := metricKey + ":" + eventID

	if exp, exists := m.records[key]; exists {
		if now.Before(exp) {
			return true, nil
		}
	}

	m.records[key] = now.Add(ttl)
	return false, nil
}

func (m *MemoryDeduplicator) Close() error {
	return nil
}
