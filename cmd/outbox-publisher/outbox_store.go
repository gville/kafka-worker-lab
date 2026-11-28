package main

import (
	"context"
	"database/sql"
	"fmt"
)

const pendingOutboxQuery = `
SELECT id, event_id, event_type, schema_version, payment_id, correlation_id, occurred_at, producer, data
FROM payment_api.outbox
WHERE published_at IS NULL
ORDER BY id ASC
LIMIT $1`

const pendingOutboxCountQuery = `
SELECT count(*) FROM payment_api.outbox
WHERE published_at IS NULL`

const markOutboxPublishedQuery = `
UPDATE payment_api.outbox
SET published_at = now()
WHERE id = $1 AND published_at IS NULL`

type outboxStore struct {
	db *sql.DB
}

func newOutboxStore(db *sql.DB) *outboxStore {
	return &outboxStore{db: db}
}

func (s *outboxStore) Pending(ctx context.Context, limit int) ([]outboxRecord, error) {
	rows, err := s.db.QueryContext(ctx, pendingOutboxQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("query pending outbox rows: %w", err)
	}
	defer rows.Close()

	records := make([]outboxRecord, 0, limit)
	for rows.Next() {
		var record outboxRecord
		if err := rows.Scan(
			&record.ID,
			&record.EventID,
			&record.EventType,
			&record.SchemaVersion,
			&record.PaymentID,
			&record.CorrelationID,
			&record.OccurredAt,
			&record.Producer,
			&record.Data,
		); err != nil {
			return nil, fmt.Errorf("scan pending outbox row: %w", err)
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending outbox rows: %w", err)
	}
	return records, nil
}

func (s *outboxStore) PendingCount(ctx context.Context) (int64, error) {
	var count int64
	if err := s.db.QueryRowContext(ctx, pendingOutboxCountQuery).Scan(&count); err != nil {
		return 0, fmt.Errorf("count pending outbox rows: %w", err)
	}
	return count, nil
}

func (s *outboxStore) MarkPublished(ctx context.Context, id int64) error {
	result, err := s.db.ExecContext(ctx, markOutboxPublishedQuery, id)
	if err != nil {
		return fmt.Errorf("mark outbox row published: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("read outbox mark result: %w", err)
	}
	if affected != 1 {
		return fmt.Errorf("unexpected outbox mark count for id %d: %d", id, affected)
	}
	return nil
}
