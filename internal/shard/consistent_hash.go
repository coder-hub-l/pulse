package shard

import (
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

// ConsistentHash implements consistent hashing with virtual nodes for
// distributing metric keys across workers. When a worker joins or leaves,
// only ~1/N of keys are remapped (where N = number of workers).
type ConsistentHash struct {
	mu           sync.RWMutex
	ring         []uint32          // Sorted hash positions on the ring
	ringMap      map[uint32]string // hash position -> worker ID
	workers      map[string]bool   // Active worker set
	virtualNodes int               // Virtual nodes per real worker (default 150)
}

func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150
	}
	return &ConsistentHash{
		ringMap:      make(map[uint32]string),
		workers:      make(map[string]bool),
		virtualNodes: virtualNodes,
	}
}

// AddWorker places a worker on the ring with its virtual nodes.
func (ch *ConsistentHash) AddWorker(workerID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if ch.workers[workerID] {
		return // Already present
	}
	ch.workers[workerID] = true

	for i := 0; i < ch.virtualNodes; i++ {
		h := ch.hash(fmt.Sprintf("%s#%d", workerID, i))
		ch.ring = append(ch.ring, h)
		ch.ringMap[h] = workerID
	}
	sort.Slice(ch.ring, func(i, j int) bool { return ch.ring[i] < ch.ring[j] })
}

// RemoveWorker removes a worker and all its virtual nodes from the ring.
func (ch *ConsistentHash) RemoveWorker(workerID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	if !ch.workers[workerID] {
		return
	}
	delete(ch.workers, workerID)

	newRing := make([]uint32, 0, len(ch.ring))
	for _, h := range ch.ring {
		if ch.ringMap[h] == workerID {
			delete(ch.ringMap, h)
		} else {
			newRing = append(newRing, h)
		}
	}
	ch.ring = newRing
}

// GetWorker returns the worker responsible for a given metric key.
func (ch *ConsistentHash) GetWorker(metricKey string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.ring) == 0 {
		return ""
	}

	h := ch.hash(metricKey)

	// Binary search for the first ring position >= h
	idx := sort.Search(len(ch.ring), func(i int) bool {
		return ch.ring[i] >= h
	})

	// Wrap around
	if idx >= len(ch.ring) {
		idx = 0
	}

	return ch.ringMap[ch.ring[idx]]
}

// GetWorkerKeys returns all metric keys (from a provided set) that map to a given worker.
func (ch *ConsistentHash) GetWorkerKeys(workerID string, allKeys []string) []string {
	var owned []string
	for _, key := range allKeys {
		if ch.GetWorker(key) == workerID {
			owned = append(owned, key)
		}
	}
	return owned
}

// Workers returns the set of active worker IDs.
func (ch *ConsistentHash) Workers() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	workers := make([]string, 0, len(ch.workers))
	for w := range ch.workers {
		workers = append(workers, w)
	}
	sort.Strings(workers)
	return workers
}

// WorkerCount returns the number of active workers.
func (ch *ConsistentHash) WorkerCount() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.workers)
}

func (ch *ConsistentHash) hash(key string) uint32 {
	h := md5.Sum([]byte(key))
	return binary.BigEndian.Uint32(h[:4])
}
