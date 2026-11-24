package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gville/kafka-worker-lab/internal/logging"
)

type statusBody struct {
	Status string `json:"status"`
}

type notReadyBody struct {
	Status string   `json:"status"`
	Failed []string `json:"failed"`
}

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("PAYMENT_API_HTTP_ADDR", "")
	t.Setenv("PAYMENT_API_DATABASE_URL", "")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.DatabaseURL != defaultDatabaseURL {
		t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, defaultDatabaseURL)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	const (
		httpAddr    = "127.0.0.1:9090"
		databaseURL = "postgres://payment_api:override_password@localhost:6543/payments_db?sslmode=disable"
	)
	t.Setenv("PAYMENT_API_HTTP_ADDR", httpAddr)
	t.Setenv("PAYMENT_API_DATABASE_URL", databaseURL)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.HTTPAddr != httpAddr {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, httpAddr)
	}
	if cfg.DatabaseURL != databaseURL {
		t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, databaseURL)
	}
}

func TestValidateConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		cfg  config
	}{
		{"empty http addr", config{HTTPAddr: " ", DatabaseURL: defaultDatabaseURL}},
		{"http addr without port", config{HTTPAddr: "localhost", DatabaseURL: defaultDatabaseURL}},
		{"zero port", config{HTTPAddr: "localhost:0", DatabaseURL: defaultDatabaseURL}},
		{"port out of range", config{HTTPAddr: "localhost:65536", DatabaseURL: defaultDatabaseURL}},
		{"empty database url", config{HTTPAddr: defaultHTTPAddr, DatabaseURL: " "}},
		{"invalid database url", config{HTTPAddr: defaultHTTPAddr, DatabaseURL: "not-a-valid-dsn"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateConfig(test.cfg); err == nil {
				t.Fatal("validateConfig() error = nil, want error")
			}
		})
	}
}

func TestValidateConfigDoesNotExposeCredentials(t *testing.T) {
	const secret = "super-secret-value"
	dsn := "postgres://payment_api:" + secret + "@localhost:5432/payments_db?sslmode=%zz"

	err := validateConfig(config{HTTPAddr: defaultHTTPAddr, DatabaseURL: dsn})
	if err == nil {
		t.Fatal("validateConfig() error = nil, want error")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), dsn) {
		t.Fatalf("validateConfig() error exposed credentials: %q", err.Error())
	}
}

func TestHealthDoesNotRunDependencyCheck(t *testing.T) {
	var calls atomic.Int32
	handler := newHealthHandler(func(context.Context) error {
		calls.Add(1)
		return nil
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var body statusBody
	decodeJSONBody(t, recorder, &body)
	if body.Status != "ok" {
		t.Fatalf("status body = %q, want %q", body.Status, "ok")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("dependency checks called %d times, want 0", got)
	}
}

func TestReadySucceedsWhenCheckPasses(t *testing.T) {
	var calls atomic.Int32
	handler := newHealthHandler(func(context.Context) error {
		calls.Add(1)
		return nil
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var body statusBody
	decodeJSONBody(t, recorder, &body)
	if body.Status != "ready" {
		t.Fatalf("status body = %q, want %q", body.Status, "ready")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("dependency checks called %d times, want 1", got)
	}
}

func TestReadyFailsWhenCheckFails(t *testing.T) {
	handler := newHealthHandler(func(context.Context) error {
		return errors.New("database unavailable")
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
	var body notReadyBody
	decodeJSONBody(t, recorder, &body)
	if body.Status != "not_ready" {
		t.Fatalf("status body = %q, want %q", body.Status, "not_ready")
	}
	if len(body.Failed) != 1 || body.Failed[0] != dependencyName {
		t.Fatalf("failed = %v, want [%s]", body.Failed, dependencyName)
	}
}

func TestShutdownForcesCloseAfterGracefulTimeout(t *testing.T) {
	originalTimeout := shutdownTimeout
	shutdownTimeout = 50 * time.Millisecond
	t.Cleanup(func() { shutdownTimeout = originalTimeout })

	handlerStarted := make(chan struct{})
	handlerReleased := make(chan struct{})
	handlerDone := make(chan struct{})

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer close(handlerDone)
			close(handlerStarted)
			<-handlerReleased
		}),
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.Serve(listener)
	}()

	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			_ = response.Body.Close()
		}
	}()

	select {
	case <-handlerStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}

	db, err := sql.Open("pgx", defaultDatabaseURL)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	logger := logging.New(logging.Config{
		Service: serviceName,
		Level:   slog.LevelInfo,
		Writer:  io.Discard,
	})

	shutdownErr := shutdown(logger, server, serverErr, db)
	if !errors.Is(shutdownErr, context.DeadlineExceeded) {
		t.Fatalf("shutdown() error = %v, want context deadline exceeded", shutdownErr)
	}

	close(handlerReleased)

	select {
	case <-clientDone:
	case <-time.After(2 * time.Second):
		t.Fatal("client connection was not closed")
	}
	select {
	case <-handlerDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return")
	}
}

func decodeJSONBody(t *testing.T, recorder *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), dst); err != nil {
		t.Fatalf("decode response body %q: %v", recorder.Body.String(), err)
	}
}
