package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/pulse/pulse-engine/internal/domain"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	ErrProducerClosed = errors.New("queue producer is closed")
)

// Producer defines the interface for publishing events to the streaming buffer.
type Producer interface {
	Publish(ctx context.Context, topic string, event domain.Event) error
	PublishBatch(ctx context.Context, topic string, events []domain.Event) (int, error)
	Close() error
}

// RedpandaProducer implements Producer using franz-go.
type RedpandaProducer struct {
	client *kgo.Client
}

// NewRedpandaProducer initializes a franz-go client with batching and zstd compression.
func NewRedpandaProducer(brokers []string) (*RedpandaProducer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.ProducerLinger(10 * time.Millisecond), // Micro-batching for high throughput
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redpanda client: %w", err)
	}

	return &RedpandaProducer{client: client}, nil
}

func (p *RedpandaProducer) Publish(ctx context.Context, topic string, event domain.Event) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(event.MetricKey), // Partition key = MetricKey ensures strict ordering
		Value: payload,
	}

	res := p.client.ProduceSync(ctx, record)
	if err := res.FirstErr(); err != nil {
		return fmt.Errorf("failed to produce record to %s: %w", topic, err)
	}

	return nil
}

func (p *RedpandaProducer) PublishBatch(ctx context.Context, topic string, events []domain.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}

	var published int
	var firstErr error

	for _, event := range events {
		payload, err := json.Marshal(event)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}

		record := &kgo.Record{
			Topic: topic,
			Key:   []byte(event.MetricKey),
			Value: payload,
		}

		p.client.Produce(ctx, record, func(r *kgo.Record, err error) {
			if err != nil && firstErr == nil {
				firstErr = err
			}
		})
		published++
	}

	if err := p.client.Flush(ctx); err != nil {
		return published, fmt.Errorf("batch flush failed: %w", err)
	}

	return published, firstErr
}

func (p *RedpandaProducer) Close() error {
	p.client.Close()
	return nil
}

// MockProducer provides an in-memory queue producer for deterministic unit testing.
type MockProducer struct {
	Records map[string][]domain.Event
	Closed  bool
}

func NewMockProducer() *MockProducer {
	return &MockProducer{
		Records: make(map[string][]domain.Event),
	}
}

func (m *MockProducer) Publish(ctx context.Context, topic string, event domain.Event) error {
	if m.Closed {
		return ErrProducerClosed
	}
	m.Records[topic] = append(m.Records[topic], event)
	return nil
}

func (m *MockProducer) PublishBatch(ctx context.Context, topic string, events []domain.Event) (int, error) {
	if m.Closed {
		return 0, ErrProducerClosed
	}
	m.Records[topic] = append(m.Records[topic], events...)
	return len(events), nil
}

func (m *MockProducer) Close() error {
	m.Closed = true
	return nil
}
