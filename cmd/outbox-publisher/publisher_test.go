package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type publisherEventKind string

const (
	queryEvent   publisherEventKind = "query"
	publishEvent publisherEventKind = "publish"
	markEvent    publisherEventKind = "mark"
)

type publisherEvent struct {
	kind     publisherEventKind
	outboxID int64
	eventID  string
	record   *kgo.Record
}

type publisherLog struct {
	mu     sync.Mutex
	events []publisherEvent
}

func (l *publisherLog) add(event publisherEvent) {
	l.mu.Lock()
	l.events = append(l.events, event)
	l.mu.Unlock()
}

func (l *publisherLog) snapshot() []publisherEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]publisherEvent(nil), l.events...)
}

func (l *publisherLog) kinds() []publisherEventKind {
	events := l.snapshot()
	kinds := make([]publisherEventKind, 0, len(events))
	for _, event := range events {
		kinds = append(kinds, event.kind)
	}
	return kinds
}

func (l *publisherLog) count(kind publisherEventKind) int {
	count := 0
	for _, eventKind := range l.kinds() {
		if eventKind == kind {
			count++
		}
	}
	return count
}

type fakeOutboxSource struct {
	log      *publisherLog
	mu       sync.Mutex
	batches  [][]outboxRecord
	calls    int
	markErr  map[int64]error
	markSeen map[int64]bool
}

func newFakeOutboxSource(log *publisherLog, batches ...[]outboxRecord) *fakeOutboxSource {
	return &fakeOutboxSource{
		log:      log,
		batches:  batches,
		markErr:  make(map[int64]error),
		markSeen: make(map[int64]bool),
	}
}

func (f *fakeOutboxSource) Pending(context.Context, int) ([]outboxRecord, error) {
	f.log.add(publisherEvent{kind: queryEvent})
	f.mu.Lock()
	defer f.mu.Unlock()
	batch := []outboxRecord(nil)
	if f.calls < len(f.batches) {
		batch = f.batches[f.calls]
	}
	f.calls++
	return batch, nil
}

func (f *fakeOutboxSource) MarkPublished(_ context.Context, id int64) error {
	f.log.add(publisherEvent{kind: markEvent, outboxID: id})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markSeen[id] = true
	return f.markErr[id]
}

func (f *fakeOutboxSource) markedIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]int64, 0, len(f.markSeen))
	for id := range f.markSeen {
		ids = append(ids, id)
	}
	return ids
}

type fakeRecordPublisher struct {
	log       *publisherLog
	mu        sync.Mutex
	errors    map[int64]error
	seen      []int64
	onPublish func()
}

func newFakeRecordPublisher(log *publisherLog) *fakeRecordPublisher {
	return &fakeRecordPublisher{
		log:    log,
		errors: make(map[int64]error),
	}
}

func (f *fakeRecordPublisher) Publish(ctx context.Context, record *kgo.Record) error {
	rowID := int64(0)
	if len(record.Key) > 0 && record.Key[0] >= '0' && record.Key[0] <= '9' {
		rowID = int64(record.Key[0] - '0')
	}
	f.log.add(publisherEvent{kind: publishEvent, outboxID: rowID, eventID: string(record.Key), record: record})
	f.mu.Lock()
	f.seen = append(f.seen, rowID)
	err := f.errors[rowID]
	onPublish := f.onPublish
	f.mu.Unlock()
	if onPublish != nil {
		onPublish()
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	return err
}

func (f *fakeRecordPublisher) publishedIDs() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int64(nil), f.seen...)
}

func testPublisher(t *testing.T, store outboxSource, producer recordPublisher) *publisher {
	t.Helper()
	return &publisher{
		store:          store,
		producer:       producer,
		logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		batchSize:      10,
		pollInterval:   time.Second,
		publishTimeout: time.Second,
	}
}

func testOutboxRow(id int64) outboxRecord {
	return outboxRecord{
		ID:            id,
		EventID:       "event-" + string(rune('0'+id)),
		EventType:     "PaymentRequested",
		SchemaVersion: 1,
		PaymentID:     string(rune('0' + id)),
		CorrelationID: "correlation-" + string(rune('0'+id)),
		OccurredAt:    time.Date(2026, time.November, 27, 12, 0, 0, 0, time.UTC),
		Producer:      "payment-api",
		Data:          []byte(`{"amountMinorUnits":100,"currency":"ARS"}`),
	}
}

func TestPublisherCyclePublishesThenMarksOneRow(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	events := log.kinds()
	if len(events) != 3 || events[0] != queryEvent || events[1] != publishEvent || events[2] != markEvent {
		t.Fatalf("event order = %v, want [query publish mark]", events)
	}
	if got := producer.publishedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published IDs = %v, want [1]", got)
	}
	if got := store.markedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("marked IDs = %v, want [1]", got)
	}
}

func TestPublisherCyclePublishesSequentiallyByID(t *testing.T) {
	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2), testOutboxRow(3)}
	store := newFakeOutboxSource(log, rows)
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	events := log.kinds()
	want := []publisherEventKind{queryEvent, publishEvent, markEvent, publishEvent, markEvent, publishEvent, markEvent}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
	if got := producer.publishedIDs(); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("published IDs = %v, want [1 2 3]", got)
	}
}

func TestPublisherCycleStopsBatchOnPublishError(t *testing.T) {
	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2), testOutboxRow(3)}
	store := newFakeOutboxSource(log, rows)
	producer := newFakeRecordPublisher(log)
	want := errors.New("broker rejected record")
	producer.errors[1] = want
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cycle() error = %v, want %v", err, want)
	}
	if got := producer.publishedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published IDs = %v, want [1]", got)
	}
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("marked IDs = %v, want none", got)
	}
}

func TestPublisherCycleRetriesPendingRowNextCycle(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row}, []outboxRecord{row})
	producer := newFakeRecordPublisher(log)
	want := errors.New("temporary publish error")
	producer.errors[1] = want
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("first cycle() error = %v, want %v", err, want)
	}
	producer.mu.Lock()
	delete(producer.errors, 1)
	producer.mu.Unlock()
	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("second cycle() error = %v", err)
	}
	if log.count(queryEvent) != 2 {
		t.Fatalf("query count = %d, want 2", log.count(queryEvent))
	}
	if got := producer.publishedIDs(); len(got) != 2 {
		t.Fatalf("published IDs = %v, want two attempts", got)
	}
	if got := store.markedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("marked IDs = %v, want [1]", got)
	}
}

func TestPublisherCycleReportsMarkErrorAfterAcknowledgedPublish(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	want := errors.New("update failed")
	store.markErr[1] = want
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cycle() error = %v, want %v", err, want)
	}
	if got := producer.publishedIDs(); len(got) != 1 {
		t.Fatalf("published IDs = %v, want one publish", got)
	}
	if got := store.markedIDs(); len(got) != 1 {
		t.Fatalf("marked IDs = %v, want one mark attempt", got)
	}
}

func TestPublisherCycleEmptyBatchIsQuiet(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log, nil)
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	if got := log.kinds(); len(got) != 1 || got[0] != queryEvent {
		t.Fatalf("events = %v, want only query", got)
	}
}

func TestPublisherRunStopsOnCancellation(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log, nil)
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)
	p.pollInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop after cancellation")
	}
}

func TestPublisherRunCancelsInFlightPublish(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)
	p.pollInterval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	producer.mu.Lock()
	producer.onPublish = func() {
		cancel()
		close(started)
	}
	producer.mu.Unlock()
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("publication did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not stop while publication was in flight")
	}
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("marked IDs after canceled publish = %v, want none", got)
	}
}

func TestPublisherDoesNotLogContextCancellationAsCycleFailure(t *testing.T) {
	var buffer strings.Builder
	logger := slog.New(slog.NewTextHandler(&buffer, nil))
	log := &publisherLog{}
	store := newFakeOutboxSource(log)
	producer := newFakeRecordPublisher(log)
	p := &publisher{
		store: store, producer: producer, logger: logger,
		batchSize: 1, pollInterval: time.Second, publishTimeout: time.Second,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.cycle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cycle() error = %v, want context.Canceled", err)
	}
	if strings.Contains(buffer.String(), "cycle failed") {
		t.Fatalf("cancellation logged as cycle failure: %s", buffer.String())
	}
}
