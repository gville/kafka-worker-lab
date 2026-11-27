package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type countingCheck struct {
	calls int32
	err   error
}

func (c *countingCheck) check(context.Context) error {
	atomic.AddInt32(&c.calls, 1)
	return c.err
}

func requestRouter(router http.Handler, path string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	return recorder
}

func TestHealthDoesNotRunDependencyChecks(t *testing.T) {
	postgres := &countingCheck{}
	kafkaCheck := &countingCheck{}
	liveness, readiness := newHealthHandlers(readinessChecks(testLogger(), postgres.check, kafkaCheck.check))
	router := newRouter(liveness, readiness)

	response := requestRouter(router, "/health")
	if response.Code != http.StatusOK {
		t.Fatalf("/health status = %d, want 200", response.Code)
	}
	if got := atomic.LoadInt32(&postgres.calls); got != 0 {
		t.Fatalf("postgres check calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&kafkaCheck.calls); got != 0 {
		t.Fatalf("kafka check calls = %d, want 0", got)
	}
}

func TestReadyRunsBothChecksAndSucceeds(t *testing.T) {
	postgres := &countingCheck{}
	kafkaCheck := &countingCheck{}
	liveness, readiness := newHealthHandlers(readinessChecks(testLogger(), postgres.check, kafkaCheck.check))
	router := newRouter(liveness, readiness)

	response := requestRouter(router, "/ready")
	if response.Code != http.StatusOK {
		t.Fatalf("/ready status = %d, want 200: %s", response.Code, response.Body.String())
	}
	if got := atomic.LoadInt32(&postgres.calls); got != 1 {
		t.Fatalf("postgres check calls = %d, want 1", got)
	}
	if got := atomic.LoadInt32(&kafkaCheck.calls); got != 1 {
		t.Fatalf("kafka check calls = %d, want 1", got)
	}
}

func TestReadyFailsWhenPostgresCheckFails(t *testing.T) {
	postgres := &countingCheck{err: errors.New("postgres unavailable")}
	kafkaCheck := &countingCheck{}
	liveness, readiness := newHealthHandlers(readinessChecks(testLogger(), postgres.check, kafkaCheck.check))
	router := newRouter(liveness, readiness)

	response := requestRouter(router, "/ready")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), postgresCheck) {
		t.Fatalf("/ready body = %s, want failed postgres dependency", response.Body.String())
	}
}

func TestReadyFailsWhenKafkaCheckFails(t *testing.T) {
	postgres := &countingCheck{}
	kafkaCheck := &countingCheck{err: errors.New("kafka unavailable")}
	liveness, readiness := newHealthHandlers(readinessChecks(testLogger(), postgres.check, kafkaCheck.check))
	router := newRouter(liveness, readiness)

	response := requestRouter(router, "/ready")
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready status = %d, want 503: %s", response.Code, response.Body.String())
	}
	if !strings.Contains(response.Body.String(), kafkaCheckName) {
		t.Fatalf("/ready body = %s, want failed kafka dependency", response.Body.String())
	}
}

func TestReadinessCheckFailuresAreLogged(t *testing.T) {
	var buffer strings.Builder
	logger := slog.New(slog.NewTextHandler(&buffer, nil))
	want := errors.New("dependency unavailable")
	checks := readinessChecks(logger, func(context.Context) error { return want }, func(context.Context) error { return nil })

	if err := checks[0].Check(context.Background()); !errors.Is(err, want) {
		t.Fatalf("postgres check error = %v, want %v", err, want)
	}
	if !strings.Contains(buffer.String(), "readiness check failed") || !strings.Contains(buffer.String(), postgresCheck) {
		t.Fatalf("readiness failure not logged: %s", buffer.String())
	}
}

type shutdownLog struct {
	mu    sync.Mutex
	steps []string
}

func (l *shutdownLog) add(step string) {
	l.mu.Lock()
	l.steps = append(l.steps, step)
	l.mu.Unlock()
}

func (l *shutdownLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.steps...)
}

type fakeCloser struct {
	log  *shutdownLog
	name string
}

func (f fakeCloser) Close() { f.log.add(f.name) }

type fakeDBCloser struct {
	log  *shutdownLog
	name string
	err  error
}

func (f fakeDBCloser) Close() error {
	f.log.add(f.name)
	return f.err
}

type fakeHTTPServer struct {
	log         *shutdownLog
	shutdownErr error
	closeErr    error
}

func (f fakeHTTPServer) Shutdown(context.Context) error {
	f.log.add("server-shutdown")
	return f.shutdownErr
}

func (f fakeHTTPServer) Close() error {
	f.log.add("server-close")
	return f.closeErr
}

type blockingHTTPServer struct {
	log     *shutdownLog
	started chan struct{}
	release chan struct{}
}

func (s blockingHTTPServer) Shutdown(context.Context) error {
	s.log.add("server-shutdown-start")
	close(s.started)
	<-s.release
	s.log.add("server-shutdown-done")
	return nil
}

func (s blockingHTTPServer) Close() error {
	s.log.add("server-close")
	return nil
}

func testShutdownDeps(log *shutdownLog, server httpShutdowner, cancel context.CancelFunc, loopDone <-chan struct{}, db dbCloser) shutdownDeps {
	return shutdownDeps{
		server:   server,
		cancel:   cancel,
		loopDone: loopDone,
		producer: fakeCloser{log: log, name: "producer"},
		db:       db,
	}
}

func assertShutdownSteps(t *testing.T, log *shutdownLog, want ...string) {
	t.Helper()
	got := log.snapshot()
	if len(got) != len(want) {
		t.Fatalf("shutdown steps = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("shutdown steps = %v, want %v", got, want)
		}
	}
}

func waitShutdownSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func TestShutdownStopsHTTPBeforeLoopAndDependencies(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		log.add("loop")
	}()
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	err := shutdown(testLogger(), testShutdownDeps(
		log,
		fakeHTTPServer{log: log},
		cancel,
		loopDone,
		fakeDBCloser{log: log, name: "db"},
	), serveResult{ch: serverErr})
	if err != nil {
		t.Fatalf("shutdown() error = %v, want nil for ErrServerClosed", err)
	}
	assertShutdownSteps(t, log, "server-shutdown", "loop", "producer", "db")
}

func TestShutdownWaitsForHTTPDrainBeforeClosingDependencies(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		log.add("loop")
	}()
	server := blockingHTTPServer{
		log:     log,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	returned := make(chan error, 1)
	go func() {
		returned <- shutdown(testLogger(), testShutdownDeps(
			log,
			server,
			cancel,
			loopDone,
			fakeDBCloser{log: log, name: "db"},
		), serveResult{ch: serverErr})
	}()

	waitShutdownSignal(t, server.started, "HTTP shutdown did not start")
	assertShutdownSteps(t, log, "server-shutdown-start")

	close(server.release)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after HTTP drain")
	}
	assertShutdownSteps(t, log, "server-shutdown-start", "server-shutdown-done", "loop", "producer", "db")
}

func TestShutdownReportsDatabaseCloseError(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		log.add("loop")
	}()
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	err := shutdown(testLogger(), testShutdownDeps(
		log,
		fakeHTTPServer{log: log},
		cancel,
		loopDone,
		fakeDBCloser{log: log, name: "db", err: errors.New("close failed")},
	), serveResult{ch: serverErr})
	if err == nil || !strings.Contains(err.Error(), "postgres connection") {
		t.Fatalf("shutdown() error = %v, want postgres close error", err)
	}
	assertShutdownSteps(t, log, "server-shutdown", "loop", "producer", "db")
}

func TestShutdownWaitsForLoopBeforeReturning(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	loopStarted := make(chan struct{})
	release := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		close(loopStarted)
		<-release
		log.add("loop")
	}()
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed

	returned := make(chan error, 1)
	go func() {
		returned <- shutdown(testLogger(), testShutdownDeps(
			log,
			fakeHTTPServer{log: log},
			cancel,
			loopDone,
			fakeDBCloser{log: log, name: "db"},
		), serveResult{ch: serverErr})
	}()

	waitShutdownSignal(t, loopStarted, "publisher was not canceled")
	select {
	case <-returned:
		t.Fatal("shutdown returned before the loop finished")
	default:
	}

	close(release)
	select {
	case err := <-returned:
		if err != nil {
			t.Fatalf("shutdown() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not return after the loop finished")
	}
	assertShutdownSteps(t, log, "server-shutdown", "loop", "producer", "db")
}

func TestShutdownForcesHTTPCloseWhenGracefulShutdownFails(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		log.add("loop")
	}()
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed
	want := errors.New("graceful shutdown failed")

	err := shutdown(testLogger(), testShutdownDeps(
		log,
		fakeHTTPServer{log: log, shutdownErr: want},
		cancel,
		loopDone,
		fakeDBCloser{log: log, name: "db"},
	), serveResult{ch: serverErr})
	if !errors.Is(err, want) {
		t.Fatalf("shutdown() error = %v, want %v", err, want)
	}
	if errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("shutdown() error = %v, must not report ErrServerClosed", err)
	}
	assertShutdownSteps(t, log, "server-shutdown", "server-close", "loop", "producer", "db")
}

func TestShutdownPreservesHTTPShutdownAndCloseErrors(t *testing.T) {
	log := &shutdownLog{}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	close(loopDone)
	serverErr := make(chan error, 1)
	serverErr <- http.ErrServerClosed
	shutdownErr := errors.New("graceful shutdown failed")
	closeErr := errors.New("forced close failed")

	err := shutdown(testLogger(), testShutdownDeps(
		log,
		fakeHTTPServer{log: log, shutdownErr: shutdownErr, closeErr: closeErr},
		cancel,
		loopDone,
		fakeDBCloser{log: log, name: "db"},
	), serveResult{ch: serverErr})
	if !errors.Is(err, shutdownErr) || !errors.Is(err, closeErr) {
		t.Fatalf("shutdown() error = %v, want both shutdown and close errors", err)
	}
	assertShutdownSteps(t, log, "server-shutdown", "server-close", "producer", "db")
}

func TestShutdownWithAlreadyReceivedServerErrorUsesSameOrder(t *testing.T) {
	log := &shutdownLog{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		<-ctx.Done()
		log.add("loop")
	}()
	want := errors.New("listen failed")

	returned := make(chan error, 1)
	go func() {
		returned <- shutdown(testLogger(), testShutdownDeps(
			log,
			fakeHTTPServer{log: log},
			cancel,
			loopDone,
			fakeDBCloser{log: log, name: "db"},
		), serveResult{err: want, have: true})
	}()

	select {
	case err := <-returned:
		if !errors.Is(err, want) {
			t.Fatalf("shutdown() error = %v, want %v", err, want)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked waiting for an already consumed server error")
	}
	assertShutdownSteps(t, log, "server-shutdown", "loop", "producer", "db")
}

func TestShutdownDoesNotReportServerClosedAsError(t *testing.T) {
	log := &shutdownLog{}
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	loopDone := make(chan struct{})
	close(loopDone)

	err := shutdown(testLogger(), testShutdownDeps(
		log,
		fakeHTTPServer{log: log},
		cancel,
		loopDone,
		fakeDBCloser{log: log, name: "db"},
	), serveResult{err: http.ErrServerClosed, have: true})
	if err != nil {
		t.Fatalf("shutdown() error = %v, want nil for ErrServerClosed", err)
	}
	assertShutdownSteps(t, log, "server-shutdown", "producer", "db")
}
