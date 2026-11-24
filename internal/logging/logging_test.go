package logging

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestInfoEmitsOneValidJSONLine(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "payment-api", Writer: &output})

	logger.Info("started")

	parseRecord(t, output.String())
}

func TestRecordContainsRequiredFields(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "payment-api", Writer: &output})

	logger.Info("started")

	record := parseRecord(t, output.String())
	assertField(t, record, "level", "INFO")
	assertField(t, record, "msg", "started")
	assertField(t, record, "service", "payment-api")
	timestamp, ok := record["timestamp"].(string)
	if !ok || timestamp == "" {
		t.Fatalf("timestamp = %v (%T), want non-empty string", record["timestamp"], record["timestamp"])
	}
}

func TestConfiguredLevelIsRespected(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "payment-worker", Level: slog.LevelWarn, Writer: &output})

	logger.Debug("hidden debug")
	logger.Info("hidden info")
	if output.Len() != 0 {
		t.Fatalf("records emitted below configured level: %q", output.String())
	}

	logger.Warn("visible warning")
	record := parseRecord(t, output.String())
	assertField(t, record, "level", "WARN")
	assertField(t, record, "msg", "visible warning")
}

func TestZeroLevelBehavesAsInfo(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "provider", Writer: &output})

	logger.Debug("hidden debug")
	if output.Len() != 0 {
		t.Fatalf("debug record emitted with zero level: %q", output.String())
	}

	logger.Info("visible info")
	record := parseRecord(t, output.String())
	assertField(t, record, "level", "INFO")
}

func TestAdditionalAttributesAreIndependentJSONFields(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "notification-worker", Writer: &output})

	logger.Info("notification simulated",
		"eventId", "event-1",
		"paymentId", "payment-1",
		"eventType", "PaymentApproved",
	)

	record := parseRecord(t, output.String())
	assertField(t, record, "eventId", "event-1")
	assertField(t, record, "paymentId", "payment-1")
	assertField(t, record, "eventType", "PaymentApproved")
}

func TestErrorRecordIsValidJSON(t *testing.T) {
	var output bytes.Buffer
	logger := New(Config{Service: "outbox-publisher", Writer: &output})

	logger.Error("publish failed", "error", errors.New("broker unavailable"))

	record := parseRecord(t, output.String())
	assertField(t, record, "level", "ERROR")
	assertField(t, record, "msg", "publish failed")
	if _, ok := record["error"]; !ok {
		t.Fatalf("error field missing from %v", record)
	}
}

func TestOutputGoesToInjectedWriter(t *testing.T) {
	var first bytes.Buffer
	var second bytes.Buffer
	firstLogger := New(Config{Service: "first", Writer: &first})
	secondLogger := New(Config{Service: "second", Writer: &second})

	firstLogger.Info("from first")
	if second.Len() != 0 {
		t.Fatalf("first logger wrote to second writer: %q", second.String())
	}

	secondLogger.Info("from second")
	firstRecord := parseRecord(t, first.String())
	secondRecord := parseRecord(t, second.String())
	assertField(t, firstRecord, "service", "first")
	assertField(t, secondRecord, "service", "second")
}

func TestDefaultWriterIsStdout(t *testing.T) {
	original := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdout pipe: %v", err)
	}
	os.Stdout = writer
	t.Cleanup(func() {
		os.Stdout = original
		_ = writer.Close()
		_ = reader.Close()
	})

	logger := New(Config{Service: "default-writer"})
	os.Stdout = original

	logger.Info("default output")

	if err := writer.Close(); err != nil {
		t.Fatalf("close pipe writer: %v", err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("read captured stdout: %v", err)
	}

	record := parseRecord(t, string(output))
	assertField(t, record, "service", "default-writer")
	assertField(t, record, "level", "INFO")
}

func parseRecord(t *testing.T, output string) map[string]any {
	t.Helper()

	if strings.Count(output, "\n") != 1 || !strings.HasSuffix(output, "\n") {
		t.Fatalf("expected exactly one newline-terminated JSON line, got %q", output)
	}
	line := strings.TrimSuffix(output, "\n")

	var record map[string]any
	if err := json.Unmarshal([]byte(line), &record); err != nil {
		t.Fatalf("decode JSON record %q: %v", output, err)
	}
	return record
}

func assertField(t *testing.T, record map[string]any, key string, want any) {
	t.Helper()

	got, ok := record[key]
	if !ok {
		t.Fatalf("field %q missing from %v", key, record)
	}
	if got != want {
		t.Fatalf("field %q = %v (%T), want %v (%T)", key, got, got, want, want)
	}
}
