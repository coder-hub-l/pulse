package shard

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// RebalanceCallback is invoked when key ownership changes.
// oldWorker may be empty for new keys, newWorker may be empty for removed workers.
type RebalanceCallback func(metricKey, oldWorker, newWorker string)

// Coordinator manages worker registration, health checks, and triggers
// rebalancing when workers join or die.
type Coordinator struct {
	mu              sync.Mutex
	ring            *ConsistentHash
	heartbeats      map[string]time.Time // workerID -> last heartbeat
	heartbeatTTL    time.Duration
	onRebalance     RebalanceCallback
	trackedKeys     map[string]string // metricKey -> current owner workerID
	logger          *slog.Logger
}

func NewCoordinator(heartbeatTTL time.Duration, virtualNodes int, onRebalance RebalanceCallback, logger *slog.Logger) *Coordinator {
	if heartbeatTTL <= 0 {
		heartbeatTTL = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Coordinator{
		ring:         NewConsistentHash(virtualNodes),
		heartbeats:   make(map[string]time.Time),
		heartbeatTTL: heartbeatTTL,
		onRebalance:  onRebalance,
		trackedKeys:  make(map[string]string),
		logger:       logger,
	}
}

// RegisterWorker adds a worker to the cluster and triggers rebalancing.
func (c *Coordinator) RegisterWorker(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ring.AddWorker(workerID)
	c.heartbeats[workerID] = time.Now().UTC()
	c.logger.Info("Worker registered", "worker_id", workerID, "total_workers", c.ring.WorkerCount())

	c.rebalanceLocked()
}

// DeregisterWorker removes a worker and triggers rebalancing.
func (c *Coordinator) DeregisterWorker(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.ring.RemoveWorker(workerID)
	delete(c.heartbeats, workerID)
	c.logger.Info("Worker deregistered", "worker_id", workerID, "total_workers", c.ring.WorkerCount())

	c.rebalanceLocked()
}

// Heartbeat updates the liveness timestamp for a worker.
func (c *Coordinator) Heartbeat(workerID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.heartbeats[workerID] = time.Now().UTC()
}

// TrackKey starts tracking a metric key for ownership changes.
func (c *Coordinator) TrackKey(metricKey string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	owner := c.ring.GetWorker(metricKey)
	c.trackedKeys[metricKey] = owner
}

// RunHealthCheck runs periodic dead worker detection.
// Call in a goroutine: go coordinator.RunHealthCheck(ctx)
func (c *Coordinator) RunHealthCheck(ctx context.Context) {
	ticker := time.NewTicker(c.heartbeatTTL / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.checkDeadWorkers()
		}
	}
}

func (c *Coordinator) checkDeadWorkers() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now().UTC()
	var dead []string

	for workerID, lastHB := range c.heartbeats {
		if now.Sub(lastHB) > c.heartbeatTTL {
			dead = append(dead, workerID)
		}
	}

	for _, workerID := range dead {
		c.ring.RemoveWorker(workerID)
		delete(c.heartbeats, workerID)
		c.logger.Warn("Worker declared dead (heartbeat timeout)", "worker_id", workerID)
	}

	if len(dead) > 0 {
		c.rebalanceLocked()
	}
}

// rebalanceLocked must be called with c.mu held.
// Computes new ownership for all tracked keys and fires callbacks for changes.
func (c *Coordinator) rebalanceLocked() {
	changes := 0
	for metricKey, oldOwner := range c.trackedKeys {
		newOwner := c.ring.GetWorker(metricKey)
		if newOwner != oldOwner {
			if c.onRebalance != nil {
				c.onRebalance(metricKey, oldOwner, newOwner)
			}
			c.trackedKeys[metricKey] = newOwner
			changes++
		}
	}
	c.logger.Info("Rebalance complete", "tracked_keys", len(c.trackedKeys), "ownership_changes", changes)
}

// GetOwner returns the current owner for a metric key.
func (c *Coordinator) GetOwner(metricKey string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ring.GetWorker(metricKey)
}

// OwnershipMap returns a snapshot of all key-to-worker assignments.
func (c *Coordinator) OwnershipMap() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := make(map[string]string, len(c.trackedKeys))
	for k, v := range c.trackedKeys {
		cp[k] = v
	}
	return cp
}

// WorkerLoad returns how many tracked keys each worker owns.
func (c *Coordinator) WorkerLoad() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()

	load := make(map[string]int)
	for _, owner := range c.trackedKeys {
		load[owner]++
	}
	return load
}

// String returns a human-readable cluster state summary.
func (c *Coordinator) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("Coordinator{workers=%d, tracked_keys=%d}", c.ring.WorkerCount(), len(c.trackedKeys))
}
