package detector

import (
	"sync"
)

// ColdStartManager handles baseline inheritance for new metric keys.
// When a metric key has insufficient history, it borrows the aggregated baseline
// from other keys sharing the same dimension group (e.g., same "region" or "category").
type ColdStartManager struct {
	mu                    sync.RWMutex
	dimensionKey          string                       // e.g. "region" — the dimension used for grouping
	groupBaselines        map[string]*SeasonalBaselines // dimensionValue -> aggregated baselines
	keyToDimensionValue   map[string]string             // metricKey -> dimensionValue
}

func NewColdStartManager(dimensionKey string) *ColdStartManager {
	return &ColdStartManager{
		dimensionKey:        dimensionKey,
		groupBaselines:      make(map[string]*SeasonalBaselines),
		keyToDimensionValue: make(map[string]string),
	}
}

// Register associates a metric key with a dimension value for cold-start grouping.
func (cs *ColdStartManager) Register(metricKey string, dimensions map[string]string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	dimVal, exists := dimensions[cs.dimensionKey]
	if !exists {
		return
	}

	cs.keyToDimensionValue[metricKey] = dimVal

	if _, exists := cs.groupBaselines[dimVal]; !exists {
		cs.groupBaselines[dimVal] = NewSeasonalBaselines("_group_" + dimVal)
	}
}

// ContributeToGroup updates the group-level aggregate baseline with a new observation.
// Called after the per-key baseline is updated, so the group always has the freshest data.
func (cs *ColdStartManager) ContributeToGroup(metricKey string, value float64, ts interface{ UTC() interface{ Hour() int; Weekday() interface{} } }) {
	// This simplified version is handled via ContributeToGroupRaw
}

// ContributeToGroupRaw updates the group baseline with raw value and time.
func (cs *ColdStartManager) ContributeToGroupRaw(metricKey string, value float64, slotIndex int) {
	cs.mu.Lock()
	defer cs.mu.Unlock()

	dimVal, exists := cs.keyToDimensionValue[metricKey]
	if !exists {
		return
	}

	groupBL, exists := cs.groupBaselines[dimVal]
	if !exists {
		return
	}

	// Directly update the slot in the group baseline
	groupBL.Slots[slotIndex].Count++
	n := float64(groupBL.Slots[slotIndex].Count)
	delta := value - groupBL.Slots[slotIndex].Mean
	groupBL.Slots[slotIndex].Mean += delta / n
	delta2 := value - groupBL.Slots[slotIndex].Mean
	groupBL.Slots[slotIndex].M2 += delta * delta2
}

// GetInheritedBaseline returns the group-level baseline for a metric key's dimension group.
// Returns nil if no group baseline exists.
func (cs *ColdStartManager) GetInheritedBaseline(metricKey string) *SeasonalBaselines {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	dimVal, exists := cs.keyToDimensionValue[metricKey]
	if !exists {
		return nil
	}

	groupBL, exists := cs.groupBaselines[dimVal]
	if !exists {
		return nil
	}

	// Return a copy so the caller doesn't mutate the group state
	copied := *groupBL
	copied.MetricKey = metricKey
	return &copied
}

// GroupSize returns how many metric keys belong to a specific dimension value group.
func (cs *ColdStartManager) GroupSize(dimensionValue string) int {
	cs.mu.RLock()
	defer cs.mu.RUnlock()

	count := 0
	for _, v := range cs.keyToDimensionValue {
		if v == dimensionValue {
			count++
		}
	}
	return count
}
