package alert

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pulse/pulse-engine/internal/domain"
)

// Engine coordinates anomaly-to-alert aggregation, fingerprint dedup, cooldowns, and sink dispatch.
type Engine struct {
	mu           sync.Mutex
	cooldowns    map[string]time.Time // fingerprint -> cooldown expiry
	cooldownDur  time.Duration
	groupBy      []string
	sinks        []Sink
	alertHistory []domain.Alert
}

// Sink is the interface for alert notification destinations.
type Sink interface {
	Send(alert domain.Alert) error
	Name() string
}

func NewEngine(cooldownDur time.Duration, groupBy []string, sinks ...Sink) *Engine {
	if cooldownDur <= 0 {
		cooldownDur = 5 * time.Minute
	}
	return &Engine{
		cooldowns:   make(map[string]time.Time),
		cooldownDur: cooldownDur,
		groupBy:     groupBy,
		sinks:       sinks,
	}
}

// Process evaluates a batch of anomalies, groups them by fingerprint, applies cooldowns,
// and dispatches deduplicated alerts to all configured sinks.
func (e *Engine) Process(anomalies []domain.Anomaly) []domain.Alert {
	if len(anomalies) == 0 {
		return nil
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UTC()

	// Group anomalies by fingerprint
	groups := make(map[string][]domain.Anomaly)
	for _, a := range anomalies {
		fp := e.fingerprint(a)
		groups[fp] = append(groups[fp], a)
	}

	var fired []domain.Alert

	for fp, groupedAnomalies := range groups {
		// Check cooldown
		if expiry, exists := e.cooldowns[fp]; exists && now.Before(expiry) {
			continue // Suppressed by cooldown
		}

		// Pick the highest severity from the group
		severity := highestSeverity(groupedAnomalies)

		// Build the alert
		first := groupedAnomalies[0]
		alert := domain.Alert{
			ID:          uuid.New().String(),
			Fingerprint: fp,
			MetricKey:   first.MetricKey,
			Title:       buildTitle(first),
			Description: buildDescription(groupedAnomalies),
			Severity:    severity,
			Status:      domain.AlertStatusFiring,
			Anomalies:   groupedAnomalies,
			Dimensions:  first.Dimensions,
			FiredAt:     now,
		}

		// Set cooldown
		e.cooldowns[fp] = now.Add(e.cooldownDur)

		// Dispatch to all sinks
		for _, sink := range e.sinks {
			_ = sink.Send(alert) // Best effort; logged inside sink
		}

		e.alertHistory = append(e.alertHistory, alert)
		fired = append(fired, alert)
	}

	// Garbage collect expired cooldowns
	for fp, expiry := range e.cooldowns {
		if now.After(expiry) {
			delete(e.cooldowns, fp)
		}
	}

	return fired
}

// History returns all alerts fired (for testing and API queries).
func (e *Engine) History() []domain.Alert {
	e.mu.Lock()
	defer e.mu.Unlock()
	cp := make([]domain.Alert, len(e.alertHistory))
	copy(cp, e.alertHistory)
	return cp
}

// ActiveCooldowns returns the count of currently active cooldown entries.
func (e *Engine) ActiveCooldowns() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := time.Now().UTC()
	count := 0
	for _, expiry := range e.cooldowns {
		if now.Before(expiry) {
			count++
		}
	}
	return count
}

func (e *Engine) fingerprint(a domain.Anomaly) string {
	parts := []string{a.MetricKey, string(a.Type)}

	// Sort groupBy keys for deterministic fingerprints
	sortedKeys := make([]string, len(e.groupBy))
	copy(sortedKeys, e.groupBy)
	sort.Strings(sortedKeys)

	for _, key := range sortedKeys {
		if key == "metric_key" {
			continue // Already included
		}
		if val, ok := a.Dimensions[key]; ok {
			parts = append(parts, key+"="+val)
		}
	}

	raw := strings.Join(parts, "|")
	hash := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", hash[:8])
}

func highestSeverity(anomalies []domain.Anomaly) domain.Severity {
	severityRank := map[domain.Severity]int{
		domain.SeverityInfo:     0,
		domain.SeverityWarning:  1,
		domain.SeverityCritical: 2,
	}
	best := domain.SeverityInfo
	for _, a := range anomalies {
		if severityRank[a.Severity] > severityRank[best] {
			best = a.Severity
		}
	}
	return best
}

func buildTitle(a domain.Anomaly) string {
	return fmt.Sprintf("[%s] %s anomaly on %s", strings.ToUpper(string(a.Severity)), a.Type, a.MetricKey)
}

func buildDescription(anomalies []domain.Anomaly) string {
	if len(anomalies) == 1 {
		a := anomalies[0]
		return fmt.Sprintf("Observed=%.2f, Expected=%.2f±%.2f, Score=%.2f", a.ObservedValue, a.ExpectedMean, a.ExpectedStdDev, a.Score)
	}
	return fmt.Sprintf("%d anomalies grouped under this root cause", len(anomalies))
}
