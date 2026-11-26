package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

const validPaymentBody = `{"amountMinorUnits":12550,"currency":"ARS"}`

type fakePaymentStore struct {
	payment Payment
	err     error
	calls   []CreatePaymentRequest
}

func (f *fakePaymentStore) CreatePayment(_ context.Context, request CreatePaymentRequest) (Payment, error) {
	f.calls = append(f.calls, request)
	if f.err != nil {
		return Payment{}, f.err
	}
	if f.payment.PaymentID != "" {
		return f.payment, nil
	}
	return Payment{
		PaymentID: "11111111-1111-4111-8111-111111111111",
		Status:    "PENDING",
	}, nil
}

type paymentResponseBody struct {
	PaymentID string `json:"paymentId"`
	Status    string `json:"status"`
}

type errorResponseBody struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func testPaymentHandler(store PaymentStore) http.Handler {
	return newPaymentsHandler(store, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}

func headerPtr(value string) *string {
	return &value
}

func paymentRequest(header *string, body string) *http.Request {
	request := httptest.NewRequest(http.MethodPost, "/payments", strings.NewReader(body))
	if header != nil {
		request.Header.Set(idempotencyKeyHeader, *header)
	}
	return request
}

func servePayment(t *testing.T, handler http.Handler, header *string, body string) *httptest.ResponseRecorder {
	t.Helper()
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, paymentRequest(header, body))
	return recorder
}

func assertPaymentError(t *testing.T, recorder *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if recorder.Code != status {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, status, recorder.Body.String())
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}
	var body errorResponseBody
	decodeJSONBody(t, recorder, &body)
	if body.Error.Code != code {
		t.Fatalf("error.code = %q, want %q", body.Error.Code, code)
	}
	if body.Error.Message == "" {
		t.Fatal("error.message must not be empty")
	}
}

func assertCreatePaymentCall(t *testing.T, call CreatePaymentRequest, key string, payment NewPayment) {
	t.Helper()
	if call.IdempotencyKey != key {
		t.Fatalf("idempotency key = %q, want %q", call.IdempotencyKey, key)
	}
	if call.Payment != payment {
		t.Fatalf("payment = %+v, want %+v", call.Payment, payment)
	}
	wantHash, err := canonicalRequestHash(payment)
	if err != nil {
		t.Fatalf("canonicalRequestHash() error = %v", err)
	}
	if call.RequestHash != wantHash {
		t.Fatalf("request hash = %x, want %x", call.RequestHash, wantHash)
	}
}

func TestCanonicalRequestHash(t *testing.T) {
	payment := NewPayment{AmountMinorUnits: 12550, Currency: "ARS"}
	got, err := canonicalRequestHash(payment)
	if err != nil {
		t.Fatalf("canonicalRequestHash() error = %v", err)
	}
	want := sha256.Sum256([]byte(`{"amountMinorUnits":12550,"currency":"ARS"}`))
	if got != want {
		t.Fatalf("hash = %x, want %x", got, want)
	}

	same, err := canonicalRequestHash(NewPayment{AmountMinorUnits: 12550, Currency: "ARS"})
	if err != nil {
		t.Fatalf("canonicalRequestHash(same) error = %v", err)
	}
	if same != got {
		t.Fatalf("same content hash = %x, want %x", same, got)
	}

	for name, changed := range map[string]NewPayment{
		"amount":   {AmountMinorUnits: 12600, Currency: "ARS"},
		"currency": {AmountMinorUnits: 12550, Currency: "USD"},
	} {
		t.Run(name, func(t *testing.T) {
			hash, err := canonicalRequestHash(changed)
			if err != nil {
				t.Fatalf("canonicalRequestHash() error = %v", err)
			}
			if hash == got {
				t.Fatalf("changed content hash = %x, want a different hash", hash)
			}
		})
	}
}

func TestPaymentSnapshotRoundTrip(t *testing.T) {
	want := paymentSnapshot{
		PaymentID: "33333333-3333-4333-8333-333333333333",
		Status:    "PENDING",
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(data) != `{"paymentId":"33333333-3333-4333-8333-333333333333","status":"PENDING"}` {
		t.Fatalf("snapshot JSON = %s", data)
	}

	var got paymentSnapshot
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if got != want {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestIsIdempotencyPKViolation(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "matching constraint",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "idempotency_pkey"},
			want: true,
		},
		{
			name: "wrapped matching constraint",
			err:  fmt.Errorf("insert idempotency: %w", &pgconn.PgError{Code: "23505", ConstraintName: "idempotency_pkey"}),
			want: true,
		},
		{
			name: "other constraint",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "payments_pkey"},
			want: false,
		},
		{
			name: "idempotency payment unique constraint",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "idempotency_payment_id_uq"},
			want: false,
		},
		{
			name: "outbox payment unique constraint",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "outbox_payment_id_uq"},
			want: false,
		},
		{
			name: "outbox event unique constraint",
			err:  &pgconn.PgError{Code: "23505", ConstraintName: "outbox_event_id_uq"},
			want: false,
		},
		{
			name: "other sqlstate",
			err:  &pgconn.PgError{Code: "23503", ConstraintName: "idempotency_pkey"},
			want: false,
		},
		{
			name: "non postgres error",
			err:  errors.New("database unavailable"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isIdempotencyPKViolation(tt.err); got != tt.want {
				t.Fatalf("isIdempotencyPKViolation() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestCreatePaymentValidCurrencies(t *testing.T) {
	tests := []struct {
		name     string
		currency string
		amount   int64
	}{
		{name: "ARS", currency: "ARS", amount: 12550},
		{name: "USD", currency: "USD", amount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakePaymentStore{
				payment: Payment{
					PaymentID: "22222222-2222-4222-8222-222222222222",
					Status:    "PENDING",
				},
			}
			body := `{"amountMinorUnits":` + strconv.FormatInt(tt.amount, 10) +
				`,"currency":"` + tt.currency + `"}`

			recorder := servePayment(t, testPaymentHandler(store), headerPtr("key-1"), body)

			if recorder.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusAccepted, recorder.Body.String())
			}
			var response paymentResponseBody
			decodeJSONBody(t, recorder, &response)
			if response.PaymentID != store.payment.PaymentID {
				t.Fatalf("paymentId = %q, want %q", response.PaymentID, store.payment.PaymentID)
			}
			if response.Status != "PENDING" {
				t.Fatalf("status = %q, want %q", response.Status, "PENDING")
			}
			if len(store.calls) != 1 {
				t.Fatalf("store calls = %d, want 1", len(store.calls))
			}
			assertCreatePaymentCall(t, store.calls[0], "key-1", NewPayment{
				AmountMinorUnits: tt.amount,
				Currency:         tt.currency,
			})
		})
	}
}

func TestCreatePaymentRequiresIdempotencyKey(t *testing.T) {
	tests := []struct {
		name   string
		header *string
	}{
		{name: "absent", header: nil},
		{name: "empty", header: headerPtr("")},
		{name: "whitespace", header: headerPtr("   \t ")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakePaymentStore{}
			recorder := servePayment(t, testPaymentHandler(store), tt.header, validPaymentBody)

			assertPaymentError(t, recorder, http.StatusBadRequest, "MISSING_IDEMPOTENCY_KEY")
			if len(store.calls) != 0 {
				t.Fatalf("store calls = %d, want 0", len(store.calls))
			}
		})
	}
}

func TestCreatePaymentRejectsInvalidContent(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "malformed JSON", body: `{"amountMinorUnits":`},
		{name: "top-level null", body: `null`},
		{name: "top-level array", body: `[]`},
		{name: "top-level string", body: `"text"`},
		{name: "top-level number", body: `123`},
		{name: "empty object", body: `{}`},
		{name: "missing currency", body: `{"amountMinorUnits":12550}`},
		{name: "missing amount", body: `{"currency":"ARS"}`},
		{name: "null amount", body: `{"amountMinorUnits":null,"currency":"ARS"}`},
		{name: "null currency", body: `{"amountMinorUnits":12550,"currency":null}`},
		{name: "string amount", body: `{"amountMinorUnits":"12550","currency":"ARS"}`},
		{name: "decimal amount", body: `{"amountMinorUnits":12550.5,"currency":"ARS"}`},
		{name: "decimal zero amount", body: `{"amountMinorUnits":1.0,"currency":"ARS"}`},
		{name: "exponent amount", body: `{"amountMinorUnits":1e2,"currency":"ARS"}`},
		{name: "zero amount", body: `{"amountMinorUnits":0,"currency":"ARS"}`},
		{name: "negative amount", body: `{"amountMinorUnits":-1,"currency":"ARS"}`},
		{name: "amount overflow", body: `{"amountMinorUnits":9223372036854775808,"currency":"ARS"}`},
		{name: "boolean amount", body: `{"amountMinorUnits":true,"currency":"ARS"}`},
		{name: "lowercase currency", body: `{"amountMinorUnits":12550,"currency":"ars"}`},
		{name: "unsupported currency", body: `{"amountMinorUnits":12550,"currency":"EUR"}`},
		{name: "short currency", body: `{"amountMinorUnits":12550,"currency":"US"}`},
		{name: "numeric currency", body: `{"amountMinorUnits":12550,"currency":123}`},
		{name: "unknown field", body: `{"amountMinorUnits":12550,"currency":"ARS","extra":true}`},
		{name: "title case amount", body: `{"AmountMinorUnits":12550,"currency":"ARS"}`},
		{name: "lowercase amount", body: `{"amountminorunits":12550,"currency":"ARS"}`},
		{name: "uppercase amount", body: `{"AMOUNTMINORUNITS":12550,"currency":"ARS"}`},
		{name: "title case currency", body: `{"amountMinorUnits":12550,"Currency":"ARS"}`},
		{name: "uppercase currency", body: `{"amountMinorUnits":12550,"CURRENCY":"ARS"}`},
		{name: "exact and variant keys", body: `{"amountMinorUnits":12550,"currency":"ARS","Currency":"EUR"}`},
		{name: "variant amount overrides exact amount", body: `{"amountMinorUnits":1,"AmountMinorUnits":2,"currency":"ARS"}`},
		{name: "trailing value", body: `{"amountMinorUnits":12550,"currency":"ARS"}{}`},
		{name: "trailing garbage", body: `{"amountMinorUnits":12550,"currency":"ARS"} trailing`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &fakePaymentStore{}
			recorder := servePayment(t, testPaymentHandler(store), headerPtr("key-1"), tt.body)

			assertPaymentError(t, recorder, http.StatusUnprocessableEntity, "INVALID_REQUEST")
			if len(store.calls) != 0 {
				t.Fatalf("store calls = %d, want 0", len(store.calls))
			}
		})
	}
}

func TestCreatePaymentInvalidBodyWithKnownKey(t *testing.T) {
	store := &fakePaymentStore{err: ErrIdempotencyConflict}

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("known-key"), `{"amountMinorUnits":0,"currency":"ARS"}`)

	assertPaymentError(t, recorder, http.StatusUnprocessableEntity, "INVALID_REQUEST")
	if len(store.calls) != 0 {
		t.Fatalf("store calls = %d, want 0", len(store.calls))
	}
}

func TestCreatePaymentConflict(t *testing.T) {
	store := &fakePaymentStore{err: ErrIdempotencyConflict}

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("same-key"), validPaymentBody)

	assertPaymentError(t, recorder, http.StatusConflict, errorCodeIdempotencyConflict)
	var body errorResponseBody
	decodeJSONBody(t, recorder, &body)
	if body.Error.Message != messageIdempotencyConflict {
		t.Fatalf("error.message = %q, want %q", body.Error.Message, messageIdempotencyConflict)
	}
	if len(store.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(store.calls))
	}
	assertCreatePaymentCall(t, store.calls[0], "same-key", NewPayment{
		AmountMinorUnits: 12550,
		Currency:         "ARS",
	})
}

func TestCreatePaymentStoreError(t *testing.T) {
	sentinel := errors.New("database unavailable")
	store := &fakePaymentStore{err: sentinel}

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("key-1"), validPaymentBody)

	assertPaymentError(t, recorder, http.StatusInternalServerError, "INTERNAL_ERROR")
	if recorder.Code == http.StatusAccepted {
		t.Fatal("store error returned HTTP 202 Accepted")
	}
	var body errorResponseBody
	decodeJSONBody(t, recorder, &body)
	if body.Error.Message != messageInternalError {
		t.Fatalf("error.message = %q, want %q", body.Error.Message, messageInternalError)
	}
	if strings.Contains(recorder.Body.String(), sentinel.Error()) {
		t.Fatalf("response leaked store error: %q", recorder.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(store.calls))
	}
}

func TestCreatePaymentDoesNotNormalizeHeader(t *testing.T) {
	store := &fakePaymentStore{}
	key := "  key with spaces  "

	recorder := servePayment(t, testPaymentHandler(store), headerPtr(key), validPaymentBody)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(store.calls))
	}
	assertCreatePaymentCall(t, store.calls[0], key, NewPayment{
		AmountMinorUnits: 12550,
		Currency:         "ARS",
	})
}

func TestCreatePaymentReplayUsesOriginalResult(t *testing.T) {
	original := Payment{
		PaymentID: "44444444-4444-4444-8444-444444444444",
		Status:    "PENDING",
	}
	store := &fakePaymentStore{payment: original}
	handler := testPaymentHandler(store)

	var responses []paymentResponseBody
	for i := 0; i < 2; i++ {
		recorder := servePayment(t, handler, headerPtr("same-key"), validPaymentBody)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("request %d status = %d, want %d; body = %q", i+1, recorder.Code, http.StatusAccepted, recorder.Body.String())
		}
		var response paymentResponseBody
		decodeJSONBody(t, recorder, &response)
		responses = append(responses, response)
	}

	want := paymentResponseBody{PaymentID: original.PaymentID, Status: original.Status}
	for i, response := range responses {
		if response != want {
			t.Fatalf("response %d = %+v, want %+v", i+1, response, want)
		}
	}
	if len(store.calls) != 2 {
		t.Fatalf("store calls = %d, want 2", len(store.calls))
	}
	for i, call := range store.calls {
		assertCreatePaymentCall(t, call, "same-key", NewPayment{
			AmountMinorUnits: 12550,
			Currency:         "ARS",
		})
		if call.RequestHash != store.calls[0].RequestHash {
			t.Fatalf("call %d hash = %x, want %x", i+1, call.RequestHash, store.calls[0].RequestHash)
		}
	}
}

func TestCreatePaymentDuplicateJSONKeysLastWins(t *testing.T) {
	store := &fakePaymentStore{}
	body := `{"amountMinorUnits":1,"amountMinorUnits":2,"currency":"USD","currency":"ARS"}`

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("key-1"), body)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(store.calls))
	}
	assertCreatePaymentCall(t, store.calls[0], "key-1", NewPayment{
		AmountMinorUnits: 2,
		Currency:         "ARS",
	})
}

func TestRouterRegistersHealthAndPayments(t *testing.T) {
	healthHandler := newHealthHandler(func(context.Context) error { return nil })
	store := &fakePaymentStore{}
	router := newRouter(healthHandler, testPaymentHandler(store))

	healthRecorder := httptest.NewRecorder()
	router.ServeHTTP(healthRecorder, httptest.NewRequest(http.MethodGet, "/health", nil))
	if healthRecorder.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", healthRecorder.Code, http.StatusOK)
	}

	paymentRecorder := httptest.NewRecorder()
	router.ServeHTTP(paymentRecorder, paymentRequest(headerPtr("key-1"), validPaymentBody))
	if paymentRecorder.Code != http.StatusAccepted {
		t.Fatalf("payments status = %d, want %d; body = %q", paymentRecorder.Code, http.StatusAccepted, paymentRecorder.Body.String())
	}

	methodRecorder := httptest.NewRecorder()
	router.ServeHTTP(methodRecorder, httptest.NewRequest(http.MethodGet, "/payments", nil))
	if methodRecorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /payments status = %d, want %d", methodRecorder.Code, http.StatusMethodNotAllowed)
	}
}
