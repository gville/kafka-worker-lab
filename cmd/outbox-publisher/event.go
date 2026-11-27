package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// outboxRecord holds exactly the columns the publisher is authorized to read.
type outboxRecord struct {
	ID            int64
	EventID       string
	EventType     string
	SchemaVersion int
	PaymentID     string
	CorrelationID string
	OccurredAt    time.Time
	Producer      string
	Data          json.RawMessage
}

// eventEnvelope is the approved wire contract for PaymentRequested.
type eventEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	SchemaVersion int             `json:"schemaVersion"`
	PaymentID     string          `json:"paymentId"`
	CorrelationID string          `json:"correlationId"`
	OccurredAt    string          `json:"occurredAt"`
	Producer      string          `json:"producer"`
	Data          json.RawMessage `json:"data"`
}

func newKafkaRecord(row outboxRecord) (*kgo.Record, error) {
	if len(row.Data) == 0 {
		return nil, errors.New("outbox row has no event data")
	}

	envelope := eventEnvelope{
		EventID:       row.EventID,
		EventType:     row.EventType,
		SchemaVersion: row.SchemaVersion,
		PaymentID:     row.PaymentID,
		CorrelationID: row.CorrelationID,
		OccurredAt:    row.OccurredAt.UTC().Format(time.RFC3339Nano),
		Producer:      row.Producer,
		Data:          row.Data,
	}
	value, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal event envelope: %w", err)
	}

	return &kgo.Record{
		Topic: paymentsTopic,
		Key:   []byte(row.PaymentID),
		Value: value,
	}, nil
}
