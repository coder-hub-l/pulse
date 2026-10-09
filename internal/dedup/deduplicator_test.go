package dedup

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryDeduplicator_Basic(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()

	// First time: not duplicate
	isDup, err := d.IsDuplicate(ctx, "retail.orders", "evt-100", 1*time.Second)
	require.NoError(t, err)
	assert.False(t, isDup)

	// Second time: duplicate
	isDup, err = d.IsDuplicate(ctx, "retail.orders", "evt-100", 1*time.Second)
	require.NoError(t, err)
	assert.True(t, isDup)

	// Different metric key: not duplicate
	isDup, err = d.IsDuplicate(ctx, "api.latency", "evt-100", 1*time.Second)
	require.NoError(t, err)
	assert.False(t, isDup)
}

func TestMemoryDeduplicator_TTL_Expiration(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()

	// Set with 20ms TTL
	isDup, err := d.IsDuplicate(ctx, "orders", "evt-temp", 20*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, isDup)

	// Immediate replay: duplicate
	isDup, err = d.IsDuplicate(ctx, "orders", "evt-temp", 20*time.Millisecond)
	require.NoError(t, err)
	assert.True(t, isDup)

	// Wait for TTL expiration
	time.Sleep(30 * time.Millisecond)

	// After expiration: treated as new
	isDup, err = d.IsDuplicate(ctx, "orders", "evt-temp", 20*time.Millisecond)
	require.NoError(t, err)
	assert.False(t, isDup)
}

func TestMemoryDeduplicator_Concurrency(t *testing.T) {
	d := NewMemoryDeduplicator()
	ctx := context.Background()
	concurrency := 50
	eventID := "evt-concurrent-1"

	var wg sync.WaitGroup
	var acceptedCount int
	var mu sync.Mutex

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			isDup, err := d.IsDuplicate(ctx, "orders", eventID, 5*time.Second)
			if err == nil && !isDup {
				mu.Lock()
				acceptedCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	assert.Equal(t, 1, acceptedCount, "Exactly one concurrent worker must acquire the idempotency key")
}
