package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealthReportsLivenessWithoutRunningChecks(t *testing.T) {
	var calls atomic.Int32
	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{
				Name: "database",
				Check: func(context.Context) error {
					calls.Add(1)
					return nil
				},
			},
		},
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/health", nil))

	assertStatus(t, recorder, http.StatusOK)
	assertJSONContentType(t, recorder)
	if got := decodeStatus(t, recorder).Status; got != "ok" {
		t.Fatalf("status = %q, want %q", got, "ok")
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("dependency checks called %d times, want 0", got)
	}
}

func TestReadySucceedsWhenAllChecksSucceed(t *testing.T) {
	var calls atomic.Int32
	check := func(context.Context) error {
		calls.Add(1)
		return nil
	}
	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{Name: "database", Check: check},
			{Name: "kafka", Check: check},
		},
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	assertStatus(t, recorder, http.StatusOK)
	assertJSONContentType(t, recorder)
	if got := decodeStatus(t, recorder).Status; got != "ready" {
		t.Fatalf("status = %q, want %q", got, "ready")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("dependency checks called %d times, want 2", got)
	}
}

func TestReadyFailsAndIdentifiesUnavailableDependency(t *testing.T) {
	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{Name: "database", Check: func(context.Context) error { return nil }},
			{Name: "kafka", Check: func(context.Context) error { return errors.New("unavailable") }},
		},
	})

	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	assertJSONContentType(t, recorder)
	body := decodeNotReady(t, recorder)
	if body.Status != "not_ready" {
		t.Fatalf("status = %q, want %q", body.Status, "not_ready")
	}
	if want := []string{"kafka"}; !slices.Equal(body.Failed, want) {
		t.Fatalf("failed = %v, want %v", body.Failed, want)
	}
}

func TestReadyFailsOnTimeout(t *testing.T) {
	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{
				Name: "slow",
				Check: func(ctx context.Context) error {
					<-ctx.Done()
					return ctx.Err()
				},
			},
		},
	})

	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("readiness handler did not return after the timeout")
	}

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	body := decodeNotReady(t, recorder)
	if body.Status != "not_ready" {
		t.Fatalf("status = %q, want %q", body.Status, "not_ready")
	}
	if want := []string{"slow"}; !slices.Equal(body.Failed, want) {
		t.Fatalf("failed = %v, want %v", body.Failed, want)
	}
}

func TestTimeoutDoesNotWaitForUnfinishedCheck(t *testing.T) {
	release := make(chan struct{})
	checkStarted := make(chan struct{})
	checkExited := make(chan struct{})
	var releaseOnce sync.Once
	releaseCheck := func() {
		releaseOnce.Do(func() { close(release) })
	}
	defer func() {
		releaseCheck()
		select {
		case <-checkExited:
		case <-time.After(time.Second):
			t.Error("blocked check goroutine did not exit after release")
		}
	}()

	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{
				Name: "stuck",
				Check: func(context.Context) error {
					close(checkStarted)
					defer close(checkExited)
					<-release
					return nil
				},
			},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx)
	recorder := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		handler.ServeHTTP(recorder, request)
		close(done)
	}()

	select {
	case <-checkStarted:
	case <-time.After(time.Second):
		t.Fatal("readiness check did not start")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("readiness handler waited for a check that never finished")
	}

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	body := decodeNotReady(t, recorder)
	if want := []string{"stuck"}; !slices.Equal(body.Failed, want) {
		t.Fatalf("failed = %v, want %v", body.Failed, want)
	}
}

func TestExpiredOrCanceledRequestContextCannotReturnReady(t *testing.T) {
	tests := []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{
			name: "canceled",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
		},
		{
			name: "expired deadline",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()

			handler := NewHandler(Config{
				Dependencies: []Dependency{
					{Name: "database", Check: func(context.Context) error { return nil }},
				},
			})
			request := httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(ctx)
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, request)

			assertStatus(t, recorder, http.StatusServiceUnavailable)
			assertJSONContentType(t, recorder)
			body := decodeNotReady(t, recorder)
			if body.Status != "not_ready" {
				t.Fatalf("status = %q, want %q", body.Status, "not_ready")
			}
		})
	}
}

type expiredDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c expiredDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }
func (c expiredDeadlineContext) Err() error                  { return nil }
func (c expiredDeadlineContext) Done() <-chan struct{}       { return nil }

func TestPastDeadlineWithoutContextErrorCannotReturnReady(t *testing.T) {
	handler := NewHandler(Config{
		Dependencies: []Dependency{
			{Name: "database", Check: func(context.Context) error { return nil }},
		},
	})
	request := httptest.NewRequest(http.MethodGet, "/ready", nil).WithContext(expiredDeadlineContext{
		Context:  context.Background(),
		deadline: time.Now().Add(-time.Second),
	})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)

	assertStatus(t, recorder, http.StatusServiceUnavailable)
	assertJSONContentType(t, recorder)
	body := decodeNotReady(t, recorder)
	if body.Status != "not_ready" {
		t.Fatalf("status = %q, want %q", body.Status, "not_ready")
	}
}

func TestReadySucceedsWithoutDependencies(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Config{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/ready", nil))

	assertStatus(t, recorder, http.StatusOK)
	assertJSONContentType(t, recorder)
	if got := decodeStatus(t, recorder).Status; got != "ready" {
		t.Fatalf("status = %q, want %q", got, "ready")
	}
}

func TestNonGETMethodReturnsMethodNotAllowed(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Config{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/health", nil))

	assertStatus(t, recorder, http.StatusMethodNotAllowed)
}

func TestUnknownPathReturnsNotFound(t *testing.T) {
	recorder := httptest.NewRecorder()
	NewHandler(Config{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/missing", nil))

	assertStatus(t, recorder, http.StatusNotFound)
}

func assertStatus(t *testing.T, recorder *httptest.ResponseRecorder, want int) {
	t.Helper()
	if recorder.Code != want {
		t.Fatalf("status code = %d, want %d; body = %s", recorder.Code, want, recorder.Body.String())
	}
}

func assertJSONContentType(t *testing.T, recorder *httptest.ResponseRecorder) {
	t.Helper()
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q, want %q", got, "application/json")
	}
}

func decodeStatus(t *testing.T, recorder *httptest.ResponseRecorder) statusResponse {
	t.Helper()
	var body statusResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return body
}

func decodeNotReady(t *testing.T, recorder *httptest.ResponseRecorder) notReadyResponse {
	t.Helper()
	var body notReadyResponse
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return body
}
