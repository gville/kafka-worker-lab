package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type publisherEventKind string

const (
	snapshotEvent publisherEventKind = "snapshot"
	queryEvent    publisherEventKind = "query"
	publishEvent  publisherEventKind = "publish"
	markEvent     publisherEventKind = "mark"
)

type publisherEvent struct {
	kind     publisherEventKind
	outboxID int64
	eventID  string
	pending  int64
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

func publishedRecords(log *publisherLog) []*kgo.Record {
	records := make([]*kgo.Record, 0)
	for _, event := range log.snapshot() {
		if event.kind == publishEvent {
			records = append(records, event.record)
		}
	}
	return records
}

func assertEventKinds(t *testing.T, log *publisherLog, want ...publisherEventKind) {
	t.Helper()
	got := log.kinds()
	if len(got) != len(want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("events = %v, want %v", got, want)
		}
	}
}

type syncBuffer struct {
	mu      sync.Mutex
	builder strings.Builder
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.builder.String()
}

func (b *syncBuffer) entries(t *testing.T) []map[string]json.RawMessage {
	t.Helper()
	entries := make([]map[string]json.RawMessage, 0)
	for _, line := range strings.Split(strings.TrimSpace(b.String()), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var entry map[string]json.RawMessage
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("decode log line %q: %v", line, err)
		}
		entries = append(entries, entry)
	}
	return entries
}

func (b *syncBuffer) messages(t *testing.T) []string {
	t.Helper()
	messages := make([]string, 0)
	for _, entry := range b.entries(t) {
		var message string
		if raw, ok := entry["msg"]; ok {
			if err := json.Unmarshal(raw, &message); err != nil {
				t.Fatalf("decode log message %s: %v", raw, err)
			}
		}
		messages = append(messages, message)
	}
	return messages
}

func countMessage(messages []string, want string) int {
	count := 0
	for _, message := range messages {
		if message == want {
			count++
		}
	}
	return count
}

func (b *syncBuffer) snapshotCounts(t *testing.T) []int64 {
	t.Helper()
	counts := make([]int64, 0)
	for _, entry := range b.entries(t) {
		var message string
		if raw, ok := entry["msg"]; ok {
			if err := json.Unmarshal(raw, &message); err != nil {
				t.Fatalf("decode log message %s: %v", raw, err)
			}
		}
		if message != "outbox pending snapshot" {
			continue
		}
		raw, ok := entry["pendingCount"]
		if !ok {
			t.Fatalf("snapshot log without pendingCount: %v", entry)
		}
		var count int64
		if err := json.Unmarshal(raw, &count); err != nil {
			t.Fatalf("pendingCount is not numeric: %s", raw)
		}
		counts = append(counts, count)
	}
	return counts
}

type fakeOutboxSource struct {
	log          *publisherLog
	mu           sync.Mutex
	batches      [][]outboxRecord
	calls        int
	pendingErrs  []error
	countValues  []int64
	countCalls   int
	countErr     error
	countHook    func()
	markErr      map[int64]error
	markSeen     map[int64]bool
	markAttempts map[int64]int
}

func newFakeOutboxSource(log *publisherLog, batches ...[]outboxRecord) *fakeOutboxSource {
	return &fakeOutboxSource{
		log:          log,
		batches:      batches,
		markErr:      make(map[int64]error),
		markSeen:     make(map[int64]bool),
		markAttempts: make(map[int64]int),
	}
}

func (f *fakeOutboxSource) Pending(_ context.Context, _ int) ([]outboxRecord, error) {
	f.log.add(publisherEvent{kind: queryEvent})
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pendingErrs) > 0 {
		err := f.pendingErrs[0]
		f.pendingErrs = f.pendingErrs[1:]
		return nil, err
	}
	batch := []outboxRecord(nil)
	if f.calls < len(f.batches) {
		batch = f.batches[f.calls]
	}
	f.calls++
	return append([]outboxRecord(nil), batch...), nil
}

func (f *fakeOutboxSource) PendingCount(context.Context) (int64, error) {
	f.mu.Lock()
	count := int64(0)
	if len(f.countValues) > 0 {
		index := f.countCalls
		if index >= len(f.countValues) {
			index = len(f.countValues) - 1
		}
		count = f.countValues[index]
	}
	f.countCalls++
	err := f.countErr
	hook := f.countHook
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	if err != nil {
		return 0, err
	}
	f.log.add(publisherEvent{kind: snapshotEvent, pending: count})
	return count, nil
}

func (f *fakeOutboxSource) MarkPublished(_ context.Context, id int64) error {
	f.log.add(publisherEvent{kind: markEvent, outboxID: id})
	f.mu.Lock()
	defer f.mu.Unlock()
	f.markAttempts[id]++
	if err := f.markErr[id]; err != nil {
		return err
	}
	f.markSeen[id] = true
	return nil
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

func (f *fakeOutboxSource) pendingCountCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.countCalls
}

func (f *fakeOutboxSource) markAttemptsFor(id int64) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.markAttempts[id]
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

func eventIDFromRecord(record *kgo.Record) string {
	var envelope struct {
		EventID string `json:"eventId"`
	}
	if err := json.Unmarshal(record.Value, &envelope); err == nil && envelope.EventID != "" {
		return envelope.EventID
	}
	return string(record.Key)
}

func fakeRowID(record *kgo.Record) int64 {
	candidates := []string{string(record.Key), eventIDFromRecord(record)}
	for _, candidate := range candidates {
		end := len(candidate)
		start := end
		for start > 0 && candidate[start-1] >= '0' && candidate[start-1] <= '9' {
			start--
		}
		if start == end {
			continue
		}
		value, err := strconv.ParseInt(candidate[start:], 10, 64)
		if err == nil {
			return value
		}
	}
	return 0
}

func (f *fakeRecordPublisher) Publish(ctx context.Context, record *kgo.Record) error {
	rowID := fakeRowID(record)
	f.log.add(publisherEvent{
		kind:     publishEvent,
		outboxID: rowID,
		eventID:  eventIDFromRecord(record),
		record:   record,
	})
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

func testJSONPublisher(t *testing.T, store outboxSource, producer recordPublisher) (*publisher, *syncBuffer) {
	t.Helper()
	buffer := &syncBuffer{}
	return &publisher{
		store:          store,
		producer:       producer,
		logger:         slog.New(slog.NewJSONHandler(buffer, nil)),
		batchSize:      10,
		pollInterval:   time.Second,
		publishTimeout: time.Second,
	}, buffer
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
	store.countValues = []int64{1, 0}
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log, snapshotEvent, queryEvent, publishEvent, markEvent, snapshotEvent)
	if got := producer.publishedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published IDs = %v, want [1]", got)
	}
	if got := store.markedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("marked IDs = %v, want [1]", got)
	}
}

func TestPublisherCycleEmitsSnapshotAfterSuccessfulMark(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	store.countValues = []int64{1, 0}
	producer := newFakeRecordPublisher(log)
	p, buffer := testJSONPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	if got, want := buffer.snapshotCounts(t), []int64{1, 0}; len(got) != len(want) {
		t.Fatalf("snapshot counts = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("snapshot counts = %v, want %v", got, want)
			}
		}
	}
	assertEventKinds(t, log, snapshotEvent, queryEvent, publishEvent, markEvent, snapshotEvent)
}

func TestPublisherCyclePublishesSequentiallyByID(t *testing.T) {
	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2), testOutboxRow(3)}
	store := newFakeOutboxSource(log, rows)
	store.countValues = []int64{3, 2, 1, 0}
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log,
		snapshotEvent, queryEvent,
		publishEvent, markEvent, snapshotEvent,
		publishEvent, markEvent, snapshotEvent,
		publishEvent, markEvent, snapshotEvent,
	)
	if got := producer.publishedIDs(); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("published IDs = %v, want [1 2 3]", got)
	}
}

func TestPublisherCycleStopsBatchOnPublishError(t *testing.T) {
	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2), testOutboxRow(3)}
	store := newFakeOutboxSource(log, rows)
	store.countErr = errors.New("select count failed")
	want := errors.New("broker rejected record")
	producer := newFakeRecordPublisher(log)
	producer.errors[1] = want
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cycle() error = %v, want %v", err, want)
	}
	assertEventKinds(t, log, queryEvent, publishEvent)
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
	if log.count(snapshotEvent) != 3 {
		t.Fatalf("snapshot count = %d, want 3", log.count(snapshotEvent))
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
	store.countErr = errors.New("select count failed")
	want := errors.New("update failed")
	store.markErr[1] = want
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cycle() error = %v, want %v", err, want)
	}
	assertEventKinds(t, log, queryEvent, publishEvent, markEvent)
	if got := producer.publishedIDs(); len(got) != 1 {
		t.Fatalf("published IDs = %v, want one publish", got)
	}
	if got := store.markAttemptsFor(1); got != 1 {
		t.Fatalf("mark attempts = %d, want 1", got)
	}
	if got := store.markedIDs(); len(got) != 0 {
		t.Fatalf("marked IDs = %v, want none after failed mark", got)
	}
}

func TestPublisherCycleEmptyBatchStillEmitsSnapshot(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log, nil)
	store.countValues = []int64{7}
	producer := newFakeRecordPublisher(log)
	p, buffer := testJSONPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log, snapshotEvent, queryEvent)
	if got := buffer.snapshotCounts(t); len(got) != 1 || got[0] != 7 {
		t.Fatalf("snapshot counts = %v, want [7]", got)
	}
}

func TestPublisherCycleEmitsSnapshotEveryCycleEvenWhenCountUnchanged(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log, nil, nil, nil)
	store.countValues = []int64{7}
	producer := newFakeRecordPublisher(log)
	p, buffer := testJSONPublisher(t, store, producer)

	for i := 0; i < 3; i++ {
		if err := p.cycle(context.Background()); err != nil {
			t.Fatalf("cycle %d error = %v", i+1, err)
		}
	}
	if got := log.count(snapshotEvent); got != 3 {
		t.Fatalf("snapshot events = %d, want 3", got)
	}
	if got, want := buffer.snapshotCounts(t), []int64{7, 7, 7}; len(got) != len(want) {
		t.Fatalf("snapshot counts = %v, want %v", got, want)
	} else {
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("snapshot counts = %v, want %v", got, want)
			}
		}
	}
}

func TestPublisherCycleCountQueryErrorDoesNotBlockBatch(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	store.countErr = errors.New("select count failed")
	producer := newFakeRecordPublisher(log)
	p, buffer := testJSONPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log, queryEvent, publishEvent, markEvent)
	if got := buffer.snapshotCounts(t); len(got) != 0 {
		t.Fatalf("snapshot logs = %v, want none", got)
	}
	if got := countMessage(buffer.messages(t), "outbox pending count query failed"); got != 2 {
		t.Fatalf("count error logs = %d, want 2", got)
	}
	if got := producer.publishedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("published IDs = %v, want [1]", got)
	}
	if got := store.markedIDs(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("marked IDs = %v, want [1]", got)
	}
	if got := store.pendingCountCalls(); got != 2 {
		t.Fatalf("count calls = %d, want 2", got)
	}
}

func TestPublisherCycleContinuesBatchWhenPostMarkCountFails(t *testing.T) {
	log := &publisherLog{}
	rows := []outboxRecord{testOutboxRow(1), testOutboxRow(2), testOutboxRow(3)}
	store := newFakeOutboxSource(log, rows)
	store.countErr = errors.New("select count failed")
	producer := newFakeRecordPublisher(log)
	p, buffer := testJSONPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log,
		queryEvent,
		publishEvent, markEvent,
		publishEvent, markEvent,
		publishEvent, markEvent,
	)
	if got := producer.publishedIDs(); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("published IDs = %v, want [1 2 3]", got)
	}
	marked := store.markedIDs()
	if len(marked) != 3 {
		t.Fatalf("marked IDs = %v, want three IDs", marked)
	}
	seen := make(map[int64]bool, len(marked))
	for _, id := range marked {
		seen[id] = true
	}
	for _, id := range []int64{1, 2, 3} {
		if !seen[id] {
			t.Fatalf("marked IDs = %v, want id %d", marked, id)
		}
	}
	if got := store.pendingCountCalls(); got != 4 {
		t.Fatalf("count calls = %d, want 4", got)
	}
	if got := buffer.snapshotCounts(t); len(got) != 0 {
		t.Fatalf("snapshot logs = %v, want none", got)
	}
	if got := countMessage(buffer.messages(t), "outbox pending count query failed"); got != 4 {
		t.Fatalf("count error logs = %d, want 4", got)
	}
}

func TestPublisherCycleRespectsCancellationDuringCountFailure(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log, []outboxRecord{testOutboxRow(1)})
	store.countErr = errors.New("select count failed")
	ctx, cancel := context.WithCancel(context.Background())
	store.countHook = cancel
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cycle() error = %v, want context.Canceled", err)
	}
	if got := log.count(queryEvent); got != 0 {
		t.Fatalf("pending query calls = %d, want 0", got)
	}
	if got := producer.publishedIDs(); len(got) != 0 {
		t.Fatalf("published IDs = %v, want none", got)
	}
	if got := store.markAttemptsFor(1); got != 0 {
		t.Fatalf("mark attempts = %d, want 0", got)
	}
}

func TestPublisherCycleEmitsSnapshotWhenKafkaIsUnavailable(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)
	store := newFakeOutboxSource(log, []outboxRecord{row})
	store.countValues = []int64{4}
	want := errors.New("broker unavailable")
	producer := newFakeRecordPublisher(log)
	producer.errors[1] = want
	p, buffer := testJSONPublisher(t, store, producer)

	if err := p.cycle(context.Background()); !errors.Is(err, want) {
		t.Fatalf("cycle() error = %v, want %v", err, want)
	}
	assertEventKinds(t, log, snapshotEvent, queryEvent, publishEvent)
	if got := buffer.snapshotCounts(t); len(got) != 1 || got[0] != 4 {
		t.Fatalf("snapshot counts = %v, want [4]", got)
	}
	if got := store.markAttemptsFor(1); got != 0 {
		t.Fatalf("mark attempts = %d, want 0", got)
	}
}

func TestPublisherCrashAfterAcknowledgementRepublishesWithoutLoss(t *testing.T) {
	log := &publisherLog{}
	row := testOutboxRow(1)

	firstStore := newFakeOutboxSource(log, []outboxRecord{row})
	firstStore.markErr[1] = errors.New("simulated crash before mark")
	firstProducer := newFakeRecordPublisher(log)
	firstPublisher := testPublisher(t, firstStore, firstProducer)
	if err := firstPublisher.cycle(context.Background()); !errors.Is(err, firstStore.markErr[1]) {
		t.Fatalf("first cycle() error = %v, want mark error", err)
	}
	if got := firstStore.markAttemptsFor(1); got != 1 {
		t.Fatalf("first mark attempts = %d, want 1", got)
	}
	if got := firstStore.markedIDs(); len(got) != 0 {
		t.Fatalf("first marked IDs = %v, want none", got)
	}

	secondStore := newFakeOutboxSource(log, []outboxRecord{row})
	secondStore.countValues = []int64{1, 0}
	secondProducer := newFakeRecordPublisher(log)
	secondPublisher := testPublisher(t, secondStore, secondProducer)
	if err := secondPublisher.cycle(context.Background()); err != nil {
		t.Fatalf("second cycle() error = %v", err)
	}

	records := publishedRecords(log)
	if len(records) != 2 {
		t.Fatalf("published records = %d, want 2", len(records))
	}
	for _, record := range records {
		if string(record.Key) != row.PaymentID {
			t.Fatalf("key = %q, want %q", record.Key, row.PaymentID)
		}
		if got := eventIDFromRecord(record); got != row.EventID {
			t.Fatalf("eventId = %q, want %q", got, row.EventID)
		}
	}
	if got := secondStore.markedIDs(); len(got) != 1 || got[0] != row.ID {
		t.Fatalf("second marked IDs = %v, want [%d]", got, row.ID)
	}
}

func TestPublisherCyclePreservesOrderForSamePaymentID(t *testing.T) {
	log := &publisherLog{}
	first := testOutboxRow(1)
	second := testOutboxRow(2)
	first.PaymentID = "payment-shared"
	second.PaymentID = "payment-shared"
	store := newFakeOutboxSource(log, []outboxRecord{first, second})
	store.countValues = []int64{2, 1, 0}
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)

	if err := p.cycle(context.Background()); err != nil {
		t.Fatalf("cycle() error = %v", err)
	}
	assertEventKinds(t, log,
		snapshotEvent, queryEvent,
		publishEvent, markEvent, snapshotEvent,
		publishEvent, markEvent, snapshotEvent,
	)
	if got := producer.publishedIDs(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("published IDs = %v, want [1 2]", got)
	}
	records := publishedRecords(log)
	if len(records) != 2 {
		t.Fatalf("published records = %d, want 2", len(records))
	}
	if got := eventIDFromRecord(records[0]); got != first.EventID {
		t.Fatalf("first eventId = %q, want %q", got, first.EventID)
	}
	if got := eventIDFromRecord(records[1]); got != second.EventID {
		t.Fatalf("second eventId = %q, want %q", got, second.EventID)
	}
	for _, record := range records {
		if string(record.Key) != "payment-shared" {
			t.Fatalf("key = %q, want payment-shared", record.Key)
		}
	}
}

func TestPublisherRunBackoffIncreasesAndResetsAfterSuccess(t *testing.T) {
	log := &publisherLog{}
	store := newFakeOutboxSource(log)
	store.pendingErrs = []error{
		errors.New("kafka unavailable"),
		errors.New("kafka unavailable"),
		errors.New("kafka unavailable"),
	}
	producer := newFakeRecordPublisher(log)
	p := testPublisher(t, store, producer)
	p.pollInterval = 10 * time.Millisecond

	waits := make([]time.Duration, 0, 4)
	p.sleep = func(_ context.Context, d time.Duration) bool {
		waits = append(waits, d)
		return len(waits) < 4
	}

	if err := p.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 10 * time.Millisecond}
	if len(waits) != len(want) {
		t.Fatalf("waits = %v, want %v", waits, want)
	}
	for i := range want {
		if waits[i] != want[i] {
			t.Fatalf("waits = %v, want %v", waits, want)
		}
	}
	if got := store.pendingCountCalls(); got != 4 {
		t.Fatalf("count calls = %d, want 4", got)
	}
}

func TestRetryDelayCapsAtThirtyTwoPollIntervals(t *testing.T) {
	log := &publisherLog{}
	p := testPublisher(t, newFakeOutboxSource(log), newFakeRecordPublisher(log))
	p.pollInterval = time.Second

	base := p.pollInterval
	want := []time.Duration{base, 2 * base, 4 * base, 8 * base, 16 * base, 32 * base, 32 * base}
	for failures, expected := range want {
		if got := p.retryDelay(failures + 1); got != expected {
			t.Fatalf("retryDelay(%d) = %v, want %v", failures+1, got, expected)
		}
	}
}

func TestSleepContextRespectsCancellationDuringWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan bool, 1)
	go func() {
		close(started)
		done <- sleepContext(ctx, time.Hour)
	}()
	<-started
	cancel()

	select {
	case continued := <-done:
		if continued {
			t.Fatal("sleepContext() continued after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("sleepContext() did not stop after cancellation")
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
