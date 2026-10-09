package shard

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsistentHash_DistributionAndMinimalRemapping(t *testing.T) {
	ch := NewConsistentHash(100)

	// Add 3 workers
	ch.AddWorker("worker-1")
	ch.AddWorker("worker-2")
	ch.AddWorker("worker-3")

	assert.Equal(t, 3, ch.WorkerCount())

	// Assign 1000 keys and record initial mapping
	keys := make([]string, 1000)
	initialMapping := make(map[string]string)
	workerCounts := make(map[string]int)

	for i := 0; i < 1000; i++ {
		keys[i] = fmt.Sprintf("metric.orders.store_%04d", i)
		worker := ch.GetWorker(keys[i])
		initialMapping[keys[i]] = worker
		workerCounts[worker]++
	}

	// Invariant 1: All 3 workers must have received a substantial portion (roughly 1/3 each)
	for w, count := range workerCounts {
		assert.True(t, count > 200 && count < 450, "Worker %s has unfair load %d", w, count)
	}

	// Add a 4th worker: Invariant 2: Only roughly ~1/4 of keys should move
	ch.AddWorker("worker-4")
	assert.Equal(t, 4, ch.WorkerCount())

	movedKeys := 0
	for _, k := range keys {
		newWorker := ch.GetWorker(k)
		if newWorker != initialMapping[k] {
			movedKeys++
			// Moved keys must only go to the new worker!
			assert.Equal(t, "worker-4", newWorker, "Key should only move to newly added worker")
		}
	}

	// Theoretical expectation: ~25% moved. Assert within [15%, 35%]
	movedPct := float64(movedKeys) / 1000.0
	assert.True(t, movedPct >= 0.15 && movedPct <= 0.35, "Moved percentage %f out of expected range", movedPct)
}

func TestCoordinator_RebalanceOnJoinAndDeath(t *testing.T) {
	rebalanceEvents := make([]string, 0)
	cb := func(metricKey, oldWorker, newWorker string) {
		rebalanceEvents = append(rebalanceEvents, fmt.Sprintf("%s:%s->%s", metricKey, oldWorker, newWorker))
	}

	coord := NewCoordinator(100*time.Millisecond, 50, cb, nil)

	// Add initial worker and track keys
	coord.RegisterWorker("w1")
	coord.TrackKey("orders")
	coord.TrackKey("revenue")

	assert.Equal(t, "w1", coord.GetOwner("orders"))
	assert.Equal(t, "w1", coord.GetOwner("revenue"))

	// Register w2 -> rebalance should occur if keys map to w2
	coord.RegisterWorker("w2")
	ownerOrders := coord.GetOwner("orders")
	assert.True(t, ownerOrders == "w1" || ownerOrders == "w2")

	// Keep w1 alive with heartbeats while w2 times out
	for i := 0; i < 3; i++ {
		time.Sleep(50 * time.Millisecond)
		coord.Heartbeat("w1")
	}
	coord.checkDeadWorkers()

	// w1 must be the only remaining worker and own both keys
	assert.Equal(t, "w1", coord.GetOwner("orders"))
	assert.Equal(t, "w1", coord.GetOwner("revenue"))
}

func TestIdempotencyGuard_ReplayDedup(t *testing.T) {
	guard := NewMemoryIdempotencyGuard()
	ctx := context.Background()

	now := time.Now().Truncate(time.Minute)

	// First attempt must succeed
	claimed, err := guard.TryClaimRollup(ctx, "orders", now, "1m")
	require.NoError(t, err)
	assert.True(t, claimed, "First rollup write must be claimed successfully")

	// Second attempt (replay) must be rejected
	claimed2, err := guard.TryClaimRollup(ctx, "orders", now, "1m")
	require.NoError(t, err)
	assert.False(t, claimed2, "Duplicate rollup write must be rejected by idempotency guard")

	// Different window must succeed
	claimedDiff, err := guard.TryClaimRollup(ctx, "orders", now.Add(time.Minute), "1m")
	require.NoError(t, err)
	assert.True(t, claimedDiff)
}
