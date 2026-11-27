package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gville/kafka-worker-lab/internal/kafka"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type recordedProducer struct {
	log      *publisherLog
	producer recordPublisher
}

func (p *recordedProducer) Publish(ctx context.Context, record *kgo.Record) error {
	rowID := int64(0)
	if len(record.Key) > 0 && record.Key[0] >= '0' && record.Key[0] <= '9' {
		rowID = int64(record.Key[0] - '0')
	}
	p.log.add(publisherEvent{kind: publishEvent, outboxID: rowID, eventID: string(record.Key), record: record})
	return p.producer.Publish(ctx, record)
}

func newKafkaTestProducer(t *testing.T) (*kfake.Cluster, *kafka.Producer) {
	t.Helper()
	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, paymentsTopic),
	)
	if err != nil {
		t.Fatalf("create fake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:  cluster.ListenAddrs(),
		ClientID: kafkaClientID,
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)
	return cluster, producer
}

func TestPublisherIntegrationMarksOnlyAfterBrokerAcknowledgement(t *testing.T) {
	cluster, producer := newKafkaTestProducer(t)
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	p := &publisher{
		store:          store,
		producer:       &recordedProducer{log: log, producer: producer},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		batchSize:      1,
		pollInterval:   time.Second,
		publishTimeout: 5 * time.Second,
	}

	release := make(chan struct{})
	entered := make(chan struct{})
	var held atomic.Bool
	var released bool
	defer func() {
		if !released {
			close(release)
		}
	}()
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if held.CompareAndSwap(false, true) {
			cluster.SleepControl(func() {
				close(entered)
				<-release
			})
		}
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cycleDone := make(chan error, 1)
	go func() { cycleDone <- p.cycle(ctx) }()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("produce request was not intercepted: %v", ctx.Err())
	}
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("marked before acknowledgement: %v", got)
	}
	var published *kgo.Record
	for _, event := range log.snapshot() {
		if event.kind == publishEvent {
			published = event.record
		}
	}
	if published == nil {
		t.Fatal("no publish event was recorded")
	}
	if published.Topic != paymentsTopic {
		t.Fatalf("topic = %q, want %q", published.Topic, paymentsTopic)
	}
	if string(published.Key) != row.PaymentID {
		t.Fatalf("key = %q, want %q", published.Key, row.PaymentID)
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(published.Value, &envelope); err != nil {
		t.Fatalf("decode published value: %v", err)
	}
	if len(envelope) != 8 {
		t.Fatalf("published envelope has %d fields, want 8", len(envelope))
	}
	var eventID string
	if err := json.Unmarshal(envelope["eventId"], &eventID); err != nil || eventID != row.EventID {
		t.Fatalf("published eventId = %q, want %q", eventID, row.EventID)
	}

	select {
	case err := <-cycleDone:
		t.Fatalf("cycle completed before acknowledgement: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	released = true
	select {
	case err := <-cycleDone:
		if err != nil {
			t.Fatalf("cycle() error = %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("cycle did not complete after acknowledgement: %v", ctx.Err())
	}
	if got := store.markedIDs(); len(got) != 1 || got[0] != row.ID {
		t.Fatalf("marked IDs = %v, want [%d]", got, row.ID)
	}
}

func TestPublisherIntegrationBrokerErrorLeavesEventPendingAndStopsBatch(t *testing.T) {
	cluster, producer := newKafkaTestProducer(t)
	fault := cluster.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: paymentsTopic,
		Err:   kerr.MessageTooLarge,
		Count: 1,
	})
	defer fault.Remove()

	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2)}
	store := newFakeOutboxSource(log, rows)
	p := &publisher{
		store:          store,
		producer:       &recordedProducer{log: log, producer: producer},
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		batchSize:      2,
		pollInterval:   time.Second,
		publishTimeout: 5 * time.Second,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := p.cycle(ctx)
	if !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("cycle() error = %v, want MessageTooLarge", err)
	}
	if got := log.count(publishEvent); got != 1 {
		t.Fatalf("publish attempts = %d, want 1", got)
	}
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("marked IDs = %v, want none", got)
	}
	if err := fault.Wait(ctx, 1); err != nil {
		t.Fatalf("waiting for fault: %v", err)
	}
}
