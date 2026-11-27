package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeStoreDriver struct {
	mu         sync.Mutex
	columns    []string
	values     [][]driver.Value
	query      string
	queryArgs  []driver.NamedValue
	queryErr   error
	execQuery  string
	execArgs   []driver.NamedValue
	execResult driver.Result
	execErr    error
}

var registeredStoreDriver = &fakeStoreDriver{}

func init() {
	sql.Register("outbox-store-fake", registeredStoreDriver)
}

func (d *fakeStoreDriver) Open(string) (driver.Conn, error) {
	return &fakeStoreConn{driver: d}, nil
}

func (d *fakeStoreDriver) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.columns = nil
	d.values = nil
	d.query = ""
	d.queryArgs = nil
	d.queryErr = nil
	d.execQuery = ""
	d.execArgs = nil
	d.execResult = nil
	d.execErr = nil
}

type fakeStoreConn struct {
	driver *fakeStoreDriver
}

func (c *fakeStoreConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected Prepare")
}

func (c *fakeStoreConn) Close() error { return nil }

func (c *fakeStoreConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected Begin")
}

func (c *fakeStoreConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	d := c.driver
	d.mu.Lock()
	defer d.mu.Unlock()
	d.query = query
	d.queryArgs = append([]driver.NamedValue(nil), args...)
	if d.queryErr != nil {
		return nil, d.queryErr
	}
	return &fakeStoreRows{
		columns: append([]string(nil), d.columns...),
		values:  d.values,
	}, nil
}

func (c *fakeStoreConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	d := c.driver
	d.mu.Lock()
	defer d.mu.Unlock()
	d.execQuery = query
	d.execArgs = append([]driver.NamedValue(nil), args...)
	if d.execErr != nil {
		return nil, d.execErr
	}
	return d.execResult, nil
}

type fakeStoreRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r *fakeStoreRows) Columns() []string { return r.columns }

func (r *fakeStoreRows) Close() error { return nil }

func (r *fakeStoreRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

type fakeStoreResult struct {
	affected int64
	err      error
}

func (r fakeStoreResult) LastInsertId() (int64, error) {
	return 0, errors.New("unexpected LastInsertId")
}

func (r fakeStoreResult) RowsAffected() (int64, error) {
	return r.affected, r.err
}

func newFakeStore(t *testing.T) *outboxStore {
	t.Helper()
	registeredStoreDriver.reset()
	t.Cleanup(registeredStoreDriver.reset)
	db, err := sql.Open("outbox-store-fake", "outbox-store-test")
	if err != nil {
		t.Fatalf("open fake db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return newOutboxStore(db)
}

func sampleStoreColumns() []string {
	return []string{
		"id", "event_id", "event_type", "schema_version", "payment_id",
		"correlation_id", "occurred_at", "producer", "data",
	}
}

func sampleStoreValues(id int64) []driver.Value {
	return []driver.Value{
		id,
		"event-" + string(rune('0'+id)),
		"PaymentRequested",
		int64(1),
		"payment-" + string(rune('0'+id)),
		"correlation-" + string(rune('0'+id)),
		time.Date(2026, time.November, 27, 12, 0, 0, 0, time.UTC),
		"payment-api",
		[]byte(`{"amountMinorUnits":100,"currency":"ARS"}`),
	}
}

func TestPendingQueriesOnlyOutboxInInsertionOrderWithLimit(t *testing.T) {
	store := newFakeStore(t)
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.columns = sampleStoreColumns()
	registeredStoreDriver.values = [][]driver.Value{sampleStoreValues(2), sampleStoreValues(1)}
	registeredStoreDriver.mu.Unlock()

	records, err := store.Pending(context.Background(), 7)
	if err != nil {
		t.Fatalf("Pending() error = %v", err)
	}
	if len(records) != 2 {
		t.Fatalf("records = %d, want 2", len(records))
	}
	if records[0].ID != 2 || records[1].ID != 1 {
		t.Fatalf("records returned in order %d, %d", records[0].ID, records[1].ID)
	}
	if string(records[0].Data) != `{"amountMinorUnits":100,"currency":"ARS"}` {
		t.Fatalf("data = %s", records[0].Data)
	}
	if records[0].Producer != "payment-api" || records[0].EventType != "PaymentRequested" || records[0].SchemaVersion != 1 {
		t.Fatalf("record metadata = %+v", records[0])
	}

	registeredStoreDriver.mu.Lock()
	defer registeredStoreDriver.mu.Unlock()
	query := strings.ToLower(registeredStoreDriver.query)
	for _, part := range []string{
		"from payment_api.outbox",
		"where published_at is null",
		"order by id asc",
		"limit $1",
	} {
		if !strings.Contains(query, part) {
			t.Fatalf("query %q does not contain %q", registeredStoreDriver.query, part)
		}
	}
	if strings.Contains(query, "payment_api.payments") || strings.Contains(query, "payment_api.idempotency") {
		t.Fatalf("query touches a forbidden table: %q", registeredStoreDriver.query)
	}
	if len(registeredStoreDriver.queryArgs) != 1 || registeredStoreDriver.queryArgs[0].Value != int64(7) {
		t.Fatalf("query args = %v, want [7]", registeredStoreDriver.queryArgs)
	}
}

func TestPendingPropagatesQueryError(t *testing.T) {
	store := newFakeStore(t)
	want := errors.New("broker not involved; database unavailable")
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.queryErr = want
	registeredStoreDriver.mu.Unlock()

	if _, err := store.Pending(context.Background(), 1); !errors.Is(err, want) {
		t.Fatalf("Pending() error = %v, want %v", err, want)
	}
}

func TestPendingPropagatesScanError(t *testing.T) {
	store := newFakeStore(t)
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.columns = sampleStoreColumns()[:8]
	registeredStoreDriver.values = [][]driver.Value{sampleStoreValues(1)[:8]}
	registeredStoreDriver.mu.Unlock()

	if _, err := store.Pending(context.Background(), 1); err == nil {
		t.Fatal("Pending() error = nil, want scan error")
	}
}

func TestMarkPublishedUsesGuardAndChecksRowsAffected(t *testing.T) {
	store := newFakeStore(t)
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.execResult = fakeStoreResult{affected: 1}
	registeredStoreDriver.mu.Unlock()

	if err := store.MarkPublished(context.Background(), 12); err != nil {
		t.Fatalf("MarkPublished() error = %v", err)
	}
	registeredStoreDriver.mu.Lock()
	defer registeredStoreDriver.mu.Unlock()
	query := strings.ToLower(registeredStoreDriver.execQuery)
	if !strings.Contains(query, "update payment_api.outbox") || !strings.Contains(query, "published_at = now()") || !strings.Contains(query, "published_at is null") {
		t.Fatalf("unexpected mark query: %q", registeredStoreDriver.execQuery)
	}
	if len(registeredStoreDriver.execArgs) != 1 || registeredStoreDriver.execArgs[0].Value != int64(12) {
		t.Fatalf("exec args = %v, want [12]", registeredStoreDriver.execArgs)
	}
}

func TestMarkPublishedRejectsUnexpectedRowsAffected(t *testing.T) {
	for _, affected := range []int64{0, 2} {
		store := newFakeStore(t)
		registeredStoreDriver.mu.Lock()
		registeredStoreDriver.execResult = fakeStoreResult{affected: affected}
		registeredStoreDriver.mu.Unlock()

		err := store.MarkPublished(context.Background(), 12)
		if err == nil || !strings.Contains(err.Error(), "unexpected outbox mark count") {
			t.Fatalf("MarkPublished() error = %v, want unexpected count error", err)
		}
	}
}

func TestMarkPublishedPropagatesExecError(t *testing.T) {
	store := newFakeStore(t)
	want := errors.New("update failed")
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.execErr = want
	registeredStoreDriver.mu.Unlock()

	if err := store.MarkPublished(context.Background(), 12); !errors.Is(err, want) {
		t.Fatalf("MarkPublished() error = %v, want %v", err, want)
	}
}

func TestStorePreservesRawJSONData(t *testing.T) {
	store := newFakeStore(t)
	raw := json.RawMessage(`{"amountMinorUnits":9007199254740991,"currency":"USD"}`)
	registeredStoreDriver.mu.Lock()
	registeredStoreDriver.columns = sampleStoreColumns()
	value := sampleStoreValues(1)
	value[8] = []byte(raw)
	registeredStoreDriver.values = [][]driver.Value{value}
	registeredStoreDriver.mu.Unlock()

	records, err := store.Pending(context.Background(), 1)
	if err != nil {
		t.Fatalf("Pending() error = %v", err)
	}
	if string(records[0].Data) != string(raw) {
		t.Fatalf("data = %s, want %s", records[0].Data, raw)
	}
	if !json.Valid(records[0].Data) {
		t.Fatalf("data is not valid JSON: %s", records[0].Data)
	}
}
