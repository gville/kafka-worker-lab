package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"
)

const (
	idempotencyKeyHeader = "Idempotency-Key"

	errorCodeMissingIdempotencyKey = "MISSING_IDEMPOTENCY_KEY"
	errorCodeInvalidRequest        = "INVALID_REQUEST"
	errorCodeIdempotencyConflict   = "IDEMPOTENCY_KEY_CONFLICT"
	errorCodeInternalError         = "INTERNAL_ERROR"

	messageMissingIdempotencyKey = "Idempotency-Key header is required"
	messageInvalidRequest        = "request body must be a JSON object with a positive integer amountMinorUnits and currency ARS or USD"
	messageIdempotencyConflict   = "Idempotency-Key was already used with different request content"
	messageInternalError         = "could not create payment"
)

var (
	errInvalidPaymentRequest = errors.New("invalid payment request")
	ErrIdempotencyConflict   = errors.New("idempotency key reused with different content")
)

// CreatePaymentRequest carries the validated request content and the
// idempotency identity to the store.
type CreatePaymentRequest struct {
	Payment        NewPayment
	IdempotencyKey string
	RequestHash    [32]byte
}

// PaymentStore creates a payment or resolves an existing idempotency record.
// Implementations must return an error unless the create has committed or an
// existing record has been read successfully.
type PaymentStore interface {
	CreatePayment(ctx context.Context, request CreatePaymentRequest) (Payment, error)
}

// NewPayment contains only the validated request fields needed for an insert.
type NewPayment struct {
	AmountMinorUnits int64
	Currency         string
}

// Payment is the creation result exposed by the HTTP response.
type Payment struct {
	PaymentID string
	Status    string
}

type canonicalPaymentRequest struct {
	AmountMinorUnits int64  `json:"amountMinorUnits"`
	Currency         string `json:"currency"`
}

type createPaymentResponse struct {
	PaymentID string `json:"paymentId"`
	Status    string `json:"status"`
}

type errorEnvelope struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

type paymentsHandler struct {
	store  PaymentStore
	logger *slog.Logger
}

func newPaymentsHandler(store PaymentStore, logger *slog.Logger) http.Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &paymentsHandler{store: store, logger: logger}
}

func (h *paymentsHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	idempotencyKey := r.Header.Get(idempotencyKeyHeader)
	if strings.TrimSpace(idempotencyKey) == "" {
		writePaymentError(w, http.StatusBadRequest, errorCodeMissingIdempotencyKey, messageMissingIdempotencyKey)
		return
	}

	payment, err := decodeCreatePaymentRequest(r.Body)
	if err != nil {
		writePaymentError(w, http.StatusUnprocessableEntity, errorCodeInvalidRequest, messageInvalidRequest)
		return
	}

	requestHash, err := canonicalRequestHash(payment)
	if err != nil {
		h.logger.Error("could not hash payment request", "error", err)
		writePaymentError(w, http.StatusInternalServerError, errorCodeInternalError, messageInternalError)
		return
	}

	created, err := h.store.CreatePayment(r.Context(), CreatePaymentRequest{
		Payment:        payment,
		IdempotencyKey: idempotencyKey,
		RequestHash:    requestHash,
	})
	if err != nil {
		if errors.Is(err, ErrIdempotencyConflict) {
			writePaymentError(w, http.StatusConflict, errorCodeIdempotencyConflict, messageIdempotencyConflict)
			return
		}
		h.logger.Error("could not create payment", "error", err)
		writePaymentError(w, http.StatusInternalServerError, errorCodeInternalError, messageInternalError)
		return
	}

	writePaymentJSON(w, http.StatusAccepted, createPaymentResponse{
		PaymentID: created.PaymentID,
		Status:    created.Status,
	})
}

func canonicalRequestHash(payment NewPayment) ([32]byte, error) {
	payload, err := json.Marshal(canonicalPaymentRequest{
		AmountMinorUnits: payment.AmountMinorUnits,
		Currency:         payment.Currency,
	})
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(payload), nil
}

func decodeCreatePaymentRequest(body io.Reader) (NewPayment, error) {
	decoder := json.NewDecoder(body)

	// encoding/json matches struct tags case-insensitively. Decoding into a raw
	// map keeps each key exactly as it appears in the document, so only the
	// approved names are accepted. Duplicate exact keys keep the last value.
	var payload map[string]json.RawMessage
	if err := decoder.Decode(&payload); err != nil {
		return NewPayment{}, errInvalidPaymentRequest
	}
	if payload == nil {
		return NewPayment{}, errInvalidPaymentRequest
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return NewPayment{}, errInvalidPaymentRequest
	}

	for key := range payload {
		if key != "amountMinorUnits" && key != "currency" {
			return NewPayment{}, errInvalidPaymentRequest
		}
	}

	amountRaw, ok := payload["amountMinorUnits"]
	if !ok {
		return NewPayment{}, errInvalidPaymentRequest
	}
	var amount int64
	if err := json.Unmarshal(amountRaw, &amount); err != nil || amount <= 0 {
		return NewPayment{}, errInvalidPaymentRequest
	}

	currencyRaw, ok := payload["currency"]
	if !ok {
		return NewPayment{}, errInvalidPaymentRequest
	}
	var currency string
	if err := json.Unmarshal(currencyRaw, &currency); err != nil {
		return NewPayment{}, errInvalidPaymentRequest
	}
	if currency != "ARS" && currency != "USD" {
		return NewPayment{}, errInvalidPaymentRequest
	}

	return NewPayment{
		AmountMinorUnits: amount,
		Currency:         currency,
	}, nil
}

func writePaymentError(w http.ResponseWriter, status int, code, message string) {
	writePaymentJSON(w, status, errorEnvelope{
		Error: errorDetail{Code: code, Message: message},
	})
}

func writePaymentJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
