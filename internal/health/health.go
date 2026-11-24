// Package health provides narrow technical helpers for service liveness and readiness.
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const readinessTimeout = 2 * time.Second

// Check reports whether a dependency is available. It must respect ctx.
type Check func(context.Context) error

// Dependency associates a logical name with a readiness check.
type Dependency struct {
	Name  string
	Check Check
}

// Config contains the dependencies a service requires to become ready.
type Config struct {
	Dependencies []Dependency
}

type checkResult struct {
	index  int
	failed bool
}

type statusResponse struct {
	Status string `json:"status"`
}

type notReadyResponse struct {
	Status string   `json:"status"`
	Failed []string `json:"failed"`
}

type errorResponse struct {
	Error string `json:"error"`
}

type handler struct {
	dependencies []Dependency
}

// NewHandler returns an http.Handler exposing GET /health and GET /ready.
func NewHandler(cfg Config) http.Handler {
	return &handler{dependencies: append([]Dependency(nil), cfg.Dependencies...)}
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/health":
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
	case "/ready":
		if r.Method != http.MethodGet {
			methodNotAllowed(w)
			return
		}
		h.serveReady(w, r)
	default:
		writeJSON(w, http.StatusNotFound, errorResponse{Error: "not_found"})
	}
}

func (h *handler) serveReady(w http.ResponseWriter, r *http.Request) {
	total := len(h.dependencies)
	if total == 0 {
		writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
	defer cancel()

	// Buffered so a check that ignores ctx can always send its result and exit.
	results := make(chan checkResult, total)
	for i, dependency := range h.dependencies {
		go func(index int, dep Dependency) {
			failed := dep.Check == nil
			if !failed {
				failed = dep.Check(ctx) != nil
			}
			results <- checkResult{index: index, failed: failed}
		}(i, dependency)
	}

	completed := make([]bool, total)
	failed := make([]bool, total)
	timedOut := false

	for received := 0; received < total; {
		select {
		case result := <-results:
			markResult(result, completed, failed)
			received++
			// The deadline or request cancellation may have expired before this
			// result was selected; do not let it turn into a ready response.
			if contextExpired(ctx) {
				markTimeoutFailures(results, completed, failed)
				timedOut = true
				received = total
			}
		case <-ctx.Done():
			markTimeoutFailures(results, completed, failed)
			timedOut = true
			received = total
		}
	}

	// The context can expire after the last result is processed, before writing.
	if !timedOut && contextExpired(ctx) {
		markTimeoutFailures(results, completed, failed)
		timedOut = true
	}

	if timedOut || anyFailed(failed) {
		writeJSON(w, http.StatusServiceUnavailable, notReadyResponse{
			Status: "not_ready",
			Failed: dependencyNames(h.dependencies, failed),
		})
		return
	}

	writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
}

func markResult(result checkResult, completed, failed []bool) {
	completed[result.index] = true
	if result.failed {
		failed[result.index] = true
	}
}

func drainResults(results <-chan checkResult, completed, failed []bool) {
	for {
		select {
		case result := <-results:
			markResult(result, completed, failed)
		default:
			return
		}
	}
}

func markTimeoutFailures(results <-chan checkResult, completed, failed []bool) {
	drainResults(results, completed, failed)
	for i := range completed {
		if !completed[i] {
			failed[i] = true
		}
	}
}

func contextExpired(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	deadline, ok := ctx.Deadline()
	return ok && !time.Now().Before(deadline)
}

func anyFailed(failed []bool) bool {
	for _, isFailed := range failed {
		if isFailed {
			return true
		}
	}
	return false
}

func dependencyNames(dependencies []Dependency, failed []bool) []string {
	names := make([]string, 0, len(failed))
	for i, isFailed := range failed {
		if isFailed {
			names = append(names, dependencies[i].Name)
		}
	}
	return names
}

func methodNotAllowed(w http.ResponseWriter) {
	w.Header().Set("Allow", http.MethodGet)
	writeJSON(w, http.StatusMethodNotAllowed, errorResponse{Error: "method_not_allowed"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
