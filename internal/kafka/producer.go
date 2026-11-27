// Package kafka provides narrow technical helpers for Kafka producers.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// Check verifies broker availability by requesting cluster metadata through
// the existing client. It publishes nothing and respects ctx.
func (p *Producer) Check(ctx context.Context) error {
	response, err := p.client.Request(ctx, &kmsg.MetadataRequest{})
	if err != nil {
		return err
	}
	metadata, ok := response.(*kmsg.MetadataResponse)
	if !ok || metadata == nil {
		return fmt.Errorf("kafka: unexpected metadata response %T", response)
	}
	if len(metadata.Brokers) == 0 {
		return errors.New("kafka: metadata response contains no brokers")
	}
	return nil
}

// Close releases the underlying client resources. It is safe to call more than once.
func (p *Producer) Close() {
	p.closeOnce.Do(p.client.Close)
}
