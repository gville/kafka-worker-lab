package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const maxBackoffFactor = 32

var maxDuration = time.Duration(1<<63 - 1)

type outboxSource interface {
	Pending(ctx context.Context, limit int) ([]outboxRecord, error)
	PendingCount(ctx context.Context) (int64, error)
	MarkPublished(ctx context.Context, id int64) error
}

type recordPublisher interface {
	Publish(ctx context.Context, record *kgo.Record) error
}

type sleepFunc func(context.Context, time.Duration) bool

type publisher struct {
	store          outboxSource
	producer       recordPublisher
	logger         *slog.Logger
	batchSize      int
	pollInterval   time.Duration
	publishTimeout time.Duration
	sleep          sleepFunc
}

// Run polls until ctx is canceled. Only one cycle runs at a time and each
// cycle publishes its batch sequentially. Failed cycles use exponential
// backoff derived from the configured poll interval; a successful cycle
// resets it.
func (p *publisher) Run(ctx context.Context) error {
	sleep := p.sleep
	if sleep == nil {
		sleep = sleepContext
	}

	failures := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		err := p.cycle(ctx)
		if ctx.Err() != nil {
			return nil
		}

		wait := p.pollInterval
		if err != nil {
			// The failed operation was already logged with its context.
			p.logger.Error("outbox cycle failed", "error", err)
			failures++
			wait = p.retryDelay(failures)
		} else {
			failures = 0
		}

		if !sleep(ctx, wait) {
			return nil
		}
	}
}

func (p *publisher) retryDelay(failures int) time.Duration {
	if failures <= 1 {
		return p.pollInterval
	}

	maxDelay := maxDuration
	if p.pollInterval <= maxDuration/maxBackoffFactor {
		maxDelay = p.pollInterval * time.Duration(maxBackoffFactor)
	}

	delay := p.pollInterval
	for i := 1; i < failures; i++ {
		if delay >= maxDelay/2 {
			return maxDelay
		}
		delay *= 2
	}
	if delay > maxDelay {
		return maxDelay
	}
	return delay
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if err := ctx.Err(); err != nil {
		return false
	}
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// emitPendingSnapshot is observability-only: a failed count is logged but
// must not affect publication of pending records.
func (p *publisher) emitPendingSnapshot(ctx context.Context) {
	count, err := p.store.PendingCount(ctx)
	if err != nil {
		p.logger.Error("outbox pending count query failed", "error", err)
		return
	}
	p.logger.Info("outbox pending snapshot", "pendingCount", count)
}

func (p *publisher) cycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.emitPendingSnapshot(ctx)
	if err := ctx.Err(); err != nil {
		return err
	}

	rows, err := p.store.Pending(ctx, p.batchSize)
	if err != nil {
		p.logger.Error("outbox pending query failed", "error", err)
		return err
	}
	p.logger.Info("outbox batch loaded", "batchRows", len(rows))
	if len(rows) == 0 {
		return nil
	}

	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}

		record, err := newKafkaRecord(row)
		if err != nil {
			p.logger.Error("outbox event construction failed",
				"outboxId", row.ID, "eventId", row.EventID, "paymentId", row.PaymentID,
				"eventType", row.EventType, "topic", paymentsTopic, "error", err)
			return err
		}

		publishCtx, cancel := context.WithTimeout(ctx, p.publishTimeout)
		err = p.producer.Publish(publishCtx, record)
		cancel()
		if err != nil {
			p.logger.Error("publication failed",
				"outboxId", row.ID, "eventId", row.EventID, "paymentId", row.PaymentID,
				"eventType", row.EventType, "topic", paymentsTopic, "error", err)
			return err
		}
		p.logger.Info("event published",
			"outboxId", row.ID, "eventId", row.EventID, "paymentId", row.PaymentID,
			"eventType", row.EventType, "topic", paymentsTopic)

		if err := p.store.MarkPublished(ctx, row.ID); err != nil {
			p.logger.Error("outbox mark failed",
				"outboxId", row.ID, "eventId", row.EventID, "paymentId", row.PaymentID,
				"eventType", row.EventType, "topic", paymentsTopic, "error", err)
			return err
		}
		p.logger.Info("outbox row marked published",
			"outboxId", row.ID, "eventId", row.EventID, "paymentId", row.PaymentID,
			"eventType", row.EventType, "topic", paymentsTopic)

		p.emitPendingSnapshot(ctx)
	}
	return nil
}
