package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
)

type postgresPaymentStore struct {
	db *sql.DB
}

func newPostgresPaymentStore(db *sql.DB) PaymentStore {
	return &postgresPaymentStore{db: db}
}

func (s *postgresPaymentStore) CreatePayment(ctx context.Context, payment NewPayment) (Payment, error) {
	paymentID, err := newUUIDv4()
	if err != nil {
		return Payment{}, fmt.Errorf("generate payment id: %w", err)
	}

	// correlation_id and status are intentionally omitted so PostgreSQL applies
	// its defaults. This single statement commits in autocommit mode.
	var status string
	err = s.db.QueryRowContext(
		ctx,
		`INSERT INTO payment_api.payments (payment_id, amount_minor_units, currency)
		 VALUES ($1, $2, $3)
		 RETURNING status`,
		paymentID,
		payment.AmountMinorUnits,
		payment.Currency,
	).Scan(&status)
	if err != nil {
		return Payment{}, fmt.Errorf("insert payment: %w", err)
	}

	return Payment{
		PaymentID: paymentID,
		Status:    status,
	}, nil
}

func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("read random bytes: %w", err)
	}

	b[6] = (b[6] & 0x0f) | 0x40 // UUID version 4.
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant.

	encoded := make([]byte, 36)
	hex.Encode(encoded[0:8], b[0:4])
	encoded[8] = '-'
	hex.Encode(encoded[9:13], b[4:6])
	encoded[13] = '-'
	hex.Encode(encoded[14:18], b[6:8])
	encoded[18] = '-'
	hex.Encode(encoded[19:23], b[8:10])
	encoded[23] = '-'
	hex.Encode(encoded[24:36], b[10:16])

	return string(encoded), nil
}
