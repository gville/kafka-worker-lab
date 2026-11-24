// Package kafka provides narrow technical helpers for Kafka producers.
package kafka

import (
	"context"
	"errors"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerConfig contains the connection settings required by a producer.
type ProducerConfig struct {
	Brokers  []string
	ClientID string
}

// Producer publishes records synchronously to Kafka.
type Producer struct {
	client    *kgo.Client
	closeOnce sync.Once
}

// NewProducer creates a durable, idempotent Kafka producer.
func NewProducer(cfg ProducerConfig) (*Producer, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka: at least one broker is required")
	}

	hasNonEmptyBroker := false
	for _, broker := range cfg.Brokers {
		if broker != "" {
			hasNonEmptyBroker = true
			break
		}
	}
	if !hasNonEmptyBroker {
		return nil, errors.New("kafka: at least one non-empty broker address is required")
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(cfg.ClientID),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Idempotent writes are enabled by default in franz-go;
		// DisableIdempotentWrite is deliberately not used.
	)
	if err != nil {
		return nil, err
	}

	return &Producer{client: client}, nil
}

// Publish sends one record and returns only after the broker acknowledges it.
// Any produce error is returned to the caller.
func (p *Producer) Publish(ctx context.Context, record *kgo.Record) error {
	if record == nil {
		return errors.New("kafka: nil record")
	}
	return p.client.ProduceSync(ctx, record).FirstErr()
}

// Close releases the underlying client resources. It is safe to call more than once.
func (p *Producer) Close() {
	p.closeOnce.Do(p.client.Close)
}
