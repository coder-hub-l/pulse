package shard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// StateRecovery handles persisting and restoring worker state (ownership, baselines)
// to Redis so that after a crash, a replacement worker can resume where the dead one left off.
type StateRecovery struct {
	client *redis.Client
	prefix string
	logger *slog.Logger
}

func NewStateRecovery(client *redis.Client, prefix string, logger *slog.Logger) *StateRecovery {
	if prefix == "" {
		prefix = "pulse:recovery"
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &StateRecovery{client: client, prefix: prefix, logger: logger}
}

// WorkerCheckpoint contains everything needed to recover a worker's state.
type WorkerCheckpoint struct {
	WorkerID    string            `json:"worker_id"`
	OwnedKeys  []string          `json:"owned_keys"`
	Offsets     map[string]int64  `json:"offsets"`     // topic:partition -> offset
	CheckpointAt time.Time       `json:"checkpoint_at"`
}

// SaveCheckpoint persists a worker's checkpoint to Redis.
func (sr *StateRecovery) SaveCheckpoint(ctx context.Context, cp WorkerCheckpoint) error {
	data, err := json.Marshal(cp)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	key := fmt.Sprintf("%s:checkpoint:%s", sr.prefix, cp.WorkerID)
	if err := sr.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}

	sr.logger.Debug("Checkpoint saved", "worker_id", cp.WorkerID, "owned_keys", len(cp.OwnedKeys))
	return nil
}

// LoadCheckpoint restores a worker's checkpoint from Redis.
func (sr *StateRecovery) LoadCheckpoint(ctx context.Context, workerID string) (*WorkerCheckpoint, error) {
	key := fmt.Sprintf("%s:checkpoint:%s", sr.prefix, workerID)
	data, err := sr.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load checkpoint: %w", err)
	}

	var cp WorkerCheckpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	sr.logger.Info("Checkpoint restored", "worker_id", workerID, "owned_keys", len(cp.OwnedKeys))
	return &cp, nil
}

// IdempotencyGuard provides exactly-once-effect semantics for rollup writes.
// Before writing a rollup batch to ClickHouse, we check/set a Redis key
// for the window boundary. If the key exists, the rollup was already written
// (likely during replay) and we skip the duplicate write.
type IdempotencyGuard struct {
	client *redis.Client
	prefix string
	ttl    time.Duration
}

func NewIdempotencyGuard(client *redis.Client, prefix string, ttl time.Duration) *IdempotencyGuard {
	if prefix == "" {
		prefix = "pulse:idem"
	}
	if ttl <= 0 {
		ttl = 24 * time.Hour
	}
	return &IdempotencyGuard{client: client, prefix: prefix, ttl: ttl}
}

// TryClaimRollup attempts to claim ownership of a rollup write for a given metric+window.
// Returns true if this is the first claim (proceed with write), false if already written.
func (ig *IdempotencyGuard) TryClaimRollup(ctx context.Context, metricKey string, windowStart time.Time, resolution string) (bool, error) {
	key := fmt.Sprintf("%s:%s:%s:%d", ig.prefix, metricKey, resolution, windowStart.Unix())
	ok, err := ig.client.SetNX(ctx, key, "1", ig.ttl).Result()
	if err != nil {
		return false, fmt.Errorf("idempotency check: %w", err)
	}
	return ok, nil
}

// MemoryIdempotencyGuard is an in-memory implementation for testing.
type MemoryIdempotencyGuard struct {
	claimed map[string]bool
}

func NewMemoryIdempotencyGuard() *MemoryIdempotencyGuard {
	return &MemoryIdempotencyGuard{claimed: make(map[string]bool)}
}

func (m *MemoryIdempotencyGuard) TryClaimRollup(ctx context.Context, metricKey string, windowStart time.Time, resolution string) (bool, error) {
	key := fmt.Sprintf("%s:%s:%d", metricKey, resolution, windowStart.Unix())
	if m.claimed[key] {
		return false, nil
	}
	m.claimed[key] = true
	return true, nil
}
