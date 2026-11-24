package kafka

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const testTopic = "producer.test"

func newTestProducer(t *testing.T) (*kfake.Cluster, *Producer) {
	t.Helper()

	cluster, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatalf("create fake cluster: %v", err)
	}
	t.Cleanup(cluster.Close)

	producer, err := NewProducer(ProducerConfig{
		Brokers:  cluster.ListenAddrs(),
		ClientID: "producer-test",
	})
	if err != nil {
		t.Fatalf("create producer: %v", err)
	}
	t.Cleanup(producer.Close)

	return cluster, producer
}

func testContext(t *testing.T) context.Context {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func newTestRecord() *kgo.Record {
	return &kgo.Record{
		Topic: testTopic,
		Key:   []byte("key"),
		Value: []byte("value"),
	}
}

func TestNewProducerUsesDurableConfiguration(t *testing.T) {
	_, producer := newTestProducer(t)

	rawAcks := producer.client.OptValue(kgo.RequiredAcks)
	acks, ok := rawAcks.(kgo.Acks)
	if !ok || acks != kgo.AllISRAcks() {
		t.Fatalf("RequiredAcks = %v (%T), want AllISRAcks", rawAcks, rawAcks)
	}

	rawDisableIdempotency := producer.client.OptValue(kgo.DisableIdempotentWrite)
	disableIdempotency, ok := rawDisableIdempotency.(bool)
	if !ok || disableIdempotency {
		t.Fatalf("DisableIdempotentWrite = %v (%T), want false", rawDisableIdempotency, rawDisableIdempotency)
	}
}

func TestNewProducerRejectsBrokersWithoutAddresses(t *testing.T) {
	tests := []struct {
		name    string
		brokers []string
	}{
		{name: "empty slice", brokers: nil},
		{name: "all empty", brokers: []string{"", ""}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer, err := NewProducer(ProducerConfig{
				Brokers:  tt.brokers,
				ClientID: "producer-test",
			})
			if err == nil {
				if producer != nil {
					producer.Close()
				}
				t.Fatal("NewProducer returned no error")
			}
			if producer != nil {
				t.Fatalf("NewProducer returned a producer with error: %v", err)
			}
		})
	}
}

func TestPublishSucceedsAfterBrokerAcknowledgement(t *testing.T) {
	_, producer := newTestProducer(t)

	if err := producer.Publish(testContext(t), newTestRecord()); err != nil {
		t.Fatalf("Publish returned error: %v", err)
	}
}

func TestPublishWaitsForHeldAcknowledgement(t *testing.T) {
	cluster, producer := newTestProducer(t)

	release := make(chan struct{})
	entered := make(chan struct{})
	released := false
	// Release the control function even if an assertion fails so cleanup can close the cluster.
	defer func() {
		if !released {
			close(release)
		}
	}()

	// Intercept only the first Produce request; all other requests pass through.
	var held atomic.Bool
	cluster.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if held.CompareAndSwap(false, true) {
			// Hold the broker response until release is closed, so no ACK reaches the producer yet.
			cluster.SleepControl(func() {
				close(entered)
				<-release
			})
		}
		return nil, nil, false
	})

	ctx := testContext(t)
	done := make(chan error, 1)
	go func() {
		done <- producer.Publish(ctx, newTestRecord())
	}()

	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatalf("produce request was not intercepted: %v", ctx.Err())
	}

	// The 100 ms timeout is a negative check that Publish is still pending,
	// not the mechanism that synchronizes the goroutines.
	select {
	case err := <-done:
		t.Fatalf("Publish returned before broker acknowledgement: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	// Release the held ACK after confirming Publish has not completed.
	close(release)
	released = true

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish returned error after acknowledgement: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("Publish did not complete after acknowledgement: %v", ctx.Err())
	}
}

func TestPublishPropagatesBrokerError(t *testing.T) {
	cluster, producer := newTestProducer(t)

	fault := cluster.Fault(kfake.Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: testTopic,
		Err:   kerr.MessageTooLarge,
		Count: 1,
	})
	defer fault.Remove()

	err := producer.Publish(testContext(t), newTestRecord())
	if !errors.Is(err, kerr.MessageTooLarge) {
		t.Fatalf("Publish error = %v, want MessageTooLarge", err)
	}
	if err := fault.Wait(testContext(t), 1); err != nil {
		t.Fatalf("waiting for fault: %v", err)
	}
}
