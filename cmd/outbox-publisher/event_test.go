package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestNewKafkaRecordBuildsApprovedEnvelope(t *testing.T) {
	occurredAt := time.Date(2026, time.November, 27, 12, 34, 56, 123000000, time.UTC)
	row := outboxRecord{
		ID:            42,
		EventID:       "11111111-2222-3333-4444-555555555555",
		EventType:     "PaymentRequested",
		SchemaVersion: 1,
		PaymentID:     "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		CorrelationID: "ffffffff-0000-1111-2222-333333333333",
		OccurredAt:    occurredAt,
		Producer:      "payment-api",
		Data:          json.RawMessage(`{"amountMinorUnits":12550,"currency":"ARS"}`),
	}

	record, err := newKafkaRecord(row)
	if err != nil {
		t.Fatalf("newKafkaRecord() error = %v", err)
	}
	if record.Topic != "payments.events" {
		t.Fatalf("Topic = %q, want payments.events", record.Topic)
	}
	if string(record.Key) != row.PaymentID {
		t.Fatalf("Key = %q, want %q", record.Key, row.PaymentID)
	}

	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if len(envelope) != 8 {
		t.Fatalf("envelope has %d fields, want 8: %s", len(envelope), record.Value)
	}
	for _, name := range []string{
		"eventId", "eventType", "schemaVersion", "paymentId",
		"correlationId", "occurredAt", "producer", "data",
	} {
		if _, ok := envelope[name]; !ok {
			t.Fatalf("envelope missing %s: %s", name, record.Value)
		}
	}

	var decoded eventEnvelope
	if err := json.Unmarshal(record.Value, &decoded); err != nil {
		t.Fatalf("decode typed envelope: %v", err)
	}
	if decoded.EventID != row.EventID || decoded.PaymentID != row.PaymentID || decoded.CorrelationID != row.CorrelationID {
		t.Fatalf("identity fields = %+v, want %+v", decoded, row)
	}
	if decoded.EventType != "PaymentRequested" || decoded.SchemaVersion != 1 || decoded.Producer != "payment-api" {
		t.Fatalf("metadata fields = %+v", decoded)
	}
	parsed, err := time.Parse(time.RFC3339Nano, decoded.OccurredAt)
	if err != nil {
		t.Fatalf("occurredAt %q is not RFC3339: %v", decoded.OccurredAt, err)
	}
	if !parsed.Equal(occurredAt) {
		t.Fatalf("occurredAt = %v, want %v", parsed, occurredAt)
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(decoded.Data, &data); err != nil {
		t.Fatalf("data is not embedded JSON: %s", decoded.Data)
	}
	if len(data) != 2 {
		t.Fatalf("data has %d fields, want 2: %s", len(data), decoded.Data)
	}
	var amount int64
	if err := json.Unmarshal(data["amountMinorUnits"], &amount); err != nil || amount != 12550 {
		t.Fatalf("amountMinorUnits = %s, want 12550", data["amountMinorUnits"])
	}
	var currency string
	if err := json.Unmarshal(data["currency"], &currency); err != nil || currency != "ARS" {
		t.Fatalf("currency = %s, want ARS", data["currency"])
	}
}

func TestNewKafkaRecordUsesProducerFromOutboxColumn(t *testing.T) {
	row := outboxRecord{
		EventID:       "event-1",
		EventType:     "PaymentRequested",
		SchemaVersion: 1,
		PaymentID:     "payment-1",
		CorrelationID: "correlation-1",
		OccurredAt:    time.Now().UTC(),
		Producer:      "custom-producer",
		Data:          json.RawMessage(`{"amountMinorUnits":1,"currency":"USD"}`),
	}

	record, err := newKafkaRecord(row)
	if err != nil {
		t.Fatalf("newKafkaRecord() error = %v", err)
	}
	var envelope eventEnvelope
	if err := json.Unmarshal(record.Value, &envelope); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if envelope.Producer != "custom-producer" {
		t.Fatalf("producer = %q, want custom-producer", envelope.Producer)
	}
}

func TestNewKafkaRecordRejectsMissingData(t *testing.T) {
	_, err := newKafkaRecord(outboxRecord{PaymentID: "payment-1"})
	if err == nil {
		t.Fatal("newKafkaRecord() error = nil, want error")
	}
}
