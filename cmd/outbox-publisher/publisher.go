package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type outboxSource interface {
	Pending(ctx context.Context, limit int) ([]outboxRecord, error)
	MarkPublished(ctx context.Context, id int64) error
}

type recordPublisher interface {
	Publish(ctx context.Context, record *kgo.Record) error
}

type publisher struct {
	store          outboxSource
	producer       recordPublisher
	logger         *slog.Logger
	batchSize      int
	pollInterval   time.Duration
	publishTimeout time.Duration
}

// Run polls until ctx is canceled. Only one cycle runs at a time and each
// cycle publishes its batch sequentially.
func (p *publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		if err := p.cycle(ctx); err != nil && ctx.Err() == nil {
			// The failed operation was already logged with its context.
			p.logger.Error("outbox cycle failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (p *publisher) cycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rows, err := p.store.Pending(ctx, p.batchSize)
	if err != nil {
		p.logger.Error("outbox pending query failed", "count", 0, "error", err)
		return err
	}
	p.logger.Info("outbox batch loaded", "count", len(rows))
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
	}
	return nil
}
