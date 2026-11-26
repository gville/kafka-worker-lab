package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

const validPaymentBody = `{"amountMinorUnits":12550,"currency":"ARS"}`

type fakePaymentStore struct {
	payment Payment
	err     error
	calls   []NewPayment
}

func (f *fakePaymentStore) CreatePayment(_ context.Context, payment NewPayment) (Payment, error) {
	f.calls = append(f.calls, payment)
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
			want := NewPayment{AmountMinorUnits: tt.amount, Currency: tt.currency}
			if store.calls[0] != want {
				t.Fatalf("store call = %+v, want %+v", store.calls[0], want)
			}
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

func TestCreatePaymentStoreError(t *testing.T) {
	sentinel := errors.New("database unavailable")
	store := &fakePaymentStore{err: sentinel}

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("key-1"), validPaymentBody)

	assertPaymentError(t, recorder, http.StatusInternalServerError, "INTERNAL_ERROR")
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

	recorder := servePayment(t, testPaymentHandler(store), headerPtr("  key with spaces  "), validPaymentBody)

	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body = %q", recorder.Code, http.StatusAccepted, recorder.Body.String())
	}
	if len(store.calls) != 1 {
		t.Fatalf("store calls = %d, want 1", len(store.calls))
	}
}

func TestCreatePaymentDoesNotReplayInThisTask(t *testing.T) {
	store := &fakePaymentStore{}
	handler := testPaymentHandler(store)

	for i := 0; i < 2; i++ {
		recorder := servePayment(t, handler, headerPtr("same-key"), validPaymentBody)
		if recorder.Code != http.StatusAccepted {
			t.Fatalf("request %d status = %d, want %d; body = %q", i+1, recorder.Code, http.StatusAccepted, recorder.Body.String())
		}
	}

	if len(store.calls) != 2 {
		t.Fatalf("store calls = %d, want 2", len(store.calls))
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
	if store.calls[0].AmountMinorUnits != 2 {
		t.Fatalf("amountMinorUnits = %d, want 2", store.calls[0].AmountMinorUnits)
	}
	if store.calls[0].Currency != "ARS" {
		t.Fatalf("currency = %q, want %q", store.calls[0].Currency, "ARS")
	}
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
