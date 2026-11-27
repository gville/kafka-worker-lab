package main

import (
	"strings"
	"testing"
	"time"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"OUTBOX_PUBLISHER_POLL_INTERVAL",
		"OUTBOX_PUBLISHER_BATCH_SIZE",
		"OUTBOX_PUBLISHER_KAFKA_BROKERS",
		"OUTBOX_PUBLISHER_DATABASE_URL",
		"OUTBOX_PUBLISHER_HTTP_ADDR",
		"OUTBOX_PUBLISHER_PUBLISH_TIMEOUT",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.PollInterval != defaultPollInterval {
		t.Fatalf("PollInterval = %v, want %v", cfg.PollInterval, defaultPollInterval)
	}
	if cfg.BatchSize != defaultBatchSize {
		t.Fatalf("BatchSize = %d, want %d", cfg.BatchSize, defaultBatchSize)
	}
	if len(cfg.KafkaBrokers) != 1 || cfg.KafkaBrokers[0] != defaultKafkaBrokers {
		t.Fatalf("KafkaBrokers = %v, want [%s]", cfg.KafkaBrokers, defaultKafkaBrokers)
	}
	if cfg.DatabaseURL != defaultDatabaseURL {
		t.Fatalf("DatabaseURL = %q, want %q", cfg.DatabaseURL, defaultDatabaseURL)
	}
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Fatalf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.PublishTimeout != defaultPublishTimeout {
		t.Fatalf("PublishTimeout = %v, want %v", cfg.PublishTimeout, defaultPublishTimeout)
	}
}

func TestLoadConfigEnvOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OUTBOX_PUBLISHER_POLL_INTERVAL", "250ms")
	t.Setenv("OUTBOX_PUBLISHER_BATCH_SIZE", "7")
	t.Setenv("OUTBOX_PUBLISHER_KAFKA_BROKERS", "broker-1:9093, broker-2:9093")
	t.Setenv("OUTBOX_PUBLISHER_DATABASE_URL", "postgres://other:secret@db:5432/payments_db?sslmode=disable")
	t.Setenv("OUTBOX_PUBLISHER_HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("OUTBOX_PUBLISHER_PUBLISH_TIMEOUT", "3s")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.PollInterval != 250*time.Millisecond {
		t.Fatalf("PollInterval = %v, want 250ms", cfg.PollInterval)
	}
	if cfg.BatchSize != 7 {
		t.Fatalf("BatchSize = %d, want 7", cfg.BatchSize)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "broker-1:9093" || cfg.KafkaBrokers[1] != "broker-2:9093" {
		t.Fatalf("KafkaBrokers = %v", cfg.KafkaBrokers)
	}
	if cfg.DatabaseURL != "postgres://other:secret@db:5432/payments_db?sslmode=disable" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.HTTPAddr != "127.0.0.1:9090" {
		t.Fatalf("HTTPAddr = %q", cfg.HTTPAddr)
	}
	if cfg.PublishTimeout != 3*time.Second {
		t.Fatalf("PublishTimeout = %v, want 3s", cfg.PublishTimeout)
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{"invalid poll interval", map[string]string{"OUTBOX_PUBLISHER_POLL_INTERVAL": "soon"}},
		{"zero poll interval", map[string]string{"OUTBOX_PUBLISHER_POLL_INTERVAL": "0s"}},
		{"negative poll interval", map[string]string{"OUTBOX_PUBLISHER_POLL_INTERVAL": "-1s"}},
		{"invalid batch size", map[string]string{"OUTBOX_PUBLISHER_BATCH_SIZE": "many"}},
		{"zero batch size", map[string]string{"OUTBOX_PUBLISHER_BATCH_SIZE": "0"}},
		{"negative batch size", map[string]string{"OUTBOX_PUBLISHER_BATCH_SIZE": "-1"}},
		{"invalid publish timeout", map[string]string{"OUTBOX_PUBLISHER_PUBLISH_TIMEOUT": "later"}},
		{"zero publish timeout", map[string]string{"OUTBOX_PUBLISHER_PUBLISH_TIMEOUT": "0s"}},
		{"empty brokers", map[string]string{"OUTBOX_PUBLISHER_KAFKA_BROKERS": " , "}},
		{"broker without port", map[string]string{"OUTBOX_PUBLISHER_KAFKA_BROKERS": "kafka"}},
		{"broker with invalid port", map[string]string{"OUTBOX_PUBLISHER_KAFKA_BROKERS": "kafka:70000"}},
		{"empty http addr", map[string]string{"OUTBOX_PUBLISHER_HTTP_ADDR": " "}},
		{"http addr without port", map[string]string{"OUTBOX_PUBLISHER_HTTP_ADDR": "localhost"}},
		{"http addr with invalid port", map[string]string{"OUTBOX_PUBLISHER_HTTP_ADDR": "localhost:65536"}},
		{"empty database url", map[string]string{"OUTBOX_PUBLISHER_DATABASE_URL": " "}},
		{"invalid database url", map[string]string{"OUTBOX_PUBLISHER_DATABASE_URL": "not-a-valid-dsn"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			for name, value := range tt.env {
				t.Setenv(name, value)
			}
			if _, err := loadConfig(); err == nil {
				t.Fatal("loadConfig() error = nil, want error")
			}
		})
	}
}

func TestLoadConfigDoesNotExposeCredentials(t *testing.T) {
	clearConfigEnv(t)
	const secret = "super-secret-value"
	t.Setenv("OUTBOX_PUBLISHER_DATABASE_URL", "postgres://outbox_publisher:"+secret+"@localhost:5432/payments_db?sslmode=%zz")

	_, err := loadConfig()
	if err == nil {
		t.Fatal("loadConfig() error = nil, want error")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("loadConfig() error exposed credentials: %q", err.Error())
	}
}
