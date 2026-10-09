package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

type EventHandler func(ctx context.Context, event domain.Event) error

// Consumer defines the interface for consuming events from partitions.
type Consumer interface {
	Start(ctx context.Context, handler EventHandler) error
	Close() error
}

// RedpandaConsumer implements Consumer using franz-go Consumer Groups.
type RedpandaConsumer struct {
	client  *kgo.Client
	groupID string
	topics  []string
	logger  *slog.Logger
}

func NewRedpandaConsumer(brokers []string, groupID string, topics []string, logger *slog.Logger) (*RedpandaConsumer, error) {
	if logger == nil {
		logger = slog.Default()
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(topics...),
		kgo.BlockRebalanceOnPoll(), // Ensure safe state handoff during rebalances
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redpanda consumer: %w", err)
	}

	return &RedpandaConsumer{
		client:  client,
		groupID: groupID,
		topics:  topics,
		logger:  logger,
	}, nil
}

func (c *RedpandaConsumer) Start(ctx context.Context, handler EventHandler) error {
	c.logger.Info("Starting Redpanda Consumer Group", "group", c.groupID, "topics", c.topics)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				c.logger.Error("Fetch error from Redpanda", "topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
			continue
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			var event domain.Event
			if err := json.Unmarshal(record.Value, &event); err != nil {
				c.logger.Warn("Failed to unmarshal record from Kafka, skipping", "topic", record.Topic, "offset", record.Offset, "err", err)
				continue
			}

			if err := handler(ctx, event); err != nil {
				c.logger.Error("Failed to handle event in pipeline", "metric_key", event.MetricKey, "err", err)
			}
		}

		// Commit offsets synchronously for at-least-once streaming semantics
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			c.logger.Error("Failed to commit offsets", "err", err)
		}
	}
}

func (c *RedpandaConsumer) Close() error {
	c.client.Close()
	return nil
}

// MockConsumer allows testing the worker pipeline without live Kafka.
type MockConsumer struct {
	Events  chan domain.Event
	mu      sync.Mutex
	running bool
}

func NewMockConsumer() *MockConsumer {
	return &MockConsumer{
		Events: make(chan domain.Event, 1000),
	}
}

func (m *MockConsumer) Push(event domain.Event) {
	m.Events <- event
}

func (m *MockConsumer) Start(ctx context.Context, handler EventHandler) error {
	m.mu.Lock()
	m.running = true
	m.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case evt, ok := <-m.Events:
			if !ok {
				return nil
			}
			if err := handler(ctx, evt); err != nil {
				return err
			}
		}
	}
}

func (m *MockConsumer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.running {
		close(m.Events)
		m.running = false
	}
	return nil
}
