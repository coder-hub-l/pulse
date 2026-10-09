package alert

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
)

// WebhookSink sends alerts via HTTP POST to a configured URL.
type WebhookSink struct {
	url    string
	client *http.Client
	logger *slog.Logger
}

func NewWebhookSink(url string, logger *slog.Logger) *WebhookSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &WebhookSink{
		url: url,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

func (w *WebhookSink) Name() string { return "webhook" }

func (w *WebhookSink) Send(alert domain.Alert) error {
	payload, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("webhook marshal error: %w", err)
	}

	resp, err := w.client.Post(w.url, "application/json", bytes.NewReader(payload))
	if err != nil {
		w.logger.Error("Webhook delivery failed", "url", w.url, "err", err)
		return fmt.Errorf("webhook POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		w.logger.Error("Webhook returned error status", "url", w.url, "status", resp.StatusCode)
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	w.logger.Info("Webhook alert delivered", "url", w.url, "alert_id", alert.ID)
	return nil
}

// SlackSink sends alerts formatted for Slack incoming webhooks.
type SlackSink struct {
	webhookURL string
	client     *http.Client
	logger     *slog.Logger
}

func NewSlackSink(webhookURL string, logger *slog.Logger) *SlackSink {
	if logger == nil {
		logger = slog.Default()
	}
	return &SlackSink{
		webhookURL: webhookURL,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		logger: logger,
	}
}

func (s *SlackSink) Name() string { return "slack_webhook" }

func (s *SlackSink) Send(alert domain.Alert) error {
	severityEmoji := map[domain.Severity]string{
		domain.SeverityInfo:     "ℹ️",
		domain.SeverityWarning:  "⚠️",
		domain.SeverityCritical: "🚨",
	}

	emoji := severityEmoji[alert.Severity]

	slackPayload := map[string]interface{}{
		"text": fmt.Sprintf("%s *%s*\n%s\n`%s` | Fired at %s",
			emoji,
			alert.Title,
			alert.Description,
			alert.MetricKey,
			alert.FiredAt.Format(time.RFC3339),
		),
	}

	payload, err := json.Marshal(slackPayload)
	if err != nil {
		return fmt.Errorf("slack marshal error: %w", err)
	}

	resp, err := s.client.Post(s.webhookURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		s.logger.Error("Slack webhook delivery failed", "err", err)
		return fmt.Errorf("slack POST failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		s.logger.Error("Slack webhook returned error status", "status", resp.StatusCode)
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}

	s.logger.Info("Slack alert delivered", "alert_id", alert.ID)
	return nil
}

// MemorySink captures alerts in-memory for testing.
type MemorySink struct {
	Alerts []domain.Alert
}

func NewMemorySink() *MemorySink {
	return &MemorySink{}
}

func (m *MemorySink) Name() string { return "memory" }

func (m *MemorySink) Send(alert domain.Alert) error {
	m.Alerts = append(m.Alerts, alert)
	return nil
}
