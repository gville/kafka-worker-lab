package main

import (
	"encoding/json"
	"testing"
)

func TestMarshalPaymentRequestedData(t *testing.T) {
	tests := []struct {
		name    string
		payment NewPayment
		want    string
	}{
		{
			name:    "ARS amount",
			payment: NewPayment{AmountMinorUnits: 12550, Currency: "ARS"},
			want:    `{"amountMinorUnits":12550,"currency":"ARS"}`,
		},
		{
			name:    "USD single minor unit",
			payment: NewPayment{AmountMinorUnits: 1, Currency: "USD"},
			want:    `{"amountMinorUnits":1,"currency":"USD"}`,
		},
		{
			name:    "large amount",
			payment: NewPayment{AmountMinorUnits: 9007199254740991, Currency: "USD"},
			want:    `{"amountMinorUnits":9007199254740991,"currency":"USD"}`,
		},
		{
			name:    "max int64 amount",
			payment: NewPayment{AmountMinorUnits: 9223372036854775807, Currency: "ARS"},
			want:    `{"amountMinorUnits":9223372036854775807,"currency":"ARS"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := marshalPaymentRequestedData(tt.payment)
			if err != nil {
				t.Fatalf("marshalPaymentRequestedData() error = %v", err)
			}
			if string(got) != tt.want {
				t.Fatalf("marshalPaymentRequestedData() = %s, want %s", got, tt.want)
			}

			var keys map[string]json.RawMessage
			if err := json.Unmarshal(got, &keys); err != nil {
				t.Fatalf("decode data %q: %v", got, err)
			}
			if len(keys) != 2 {
				t.Fatalf("data has %d keys, want exactly 2: %s", len(keys), got)
			}
			if _, ok := keys["amountMinorUnits"]; !ok {
				t.Fatalf("data missing amountMinorUnits: %s", got)
			}
			if _, ok := keys["currency"]; !ok {
				t.Fatalf("data missing currency: %s", got)
			}
		})
	}
}

func TestNewPostgresPaymentStoreLeavesInjectionDisabled(t *testing.T) {
	store, ok := newPostgresPaymentStore(nil).(*postgresPaymentStore)
	if !ok {
		t.Fatal("newPostgresPaymentStore did not return *postgresPaymentStore")
	}
	if store.outboxWriteErr != nil {
		t.Fatalf("outboxWriteErr = %v, want nil", store.outboxWriteErr)
	}
}
