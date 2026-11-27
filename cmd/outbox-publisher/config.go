package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	serviceName    = "outbox-publisher"
	kafkaClientID  = "outbox-publisher"
	paymentsTopic  = "payments.events"
	postgresCheck  = "postgres"
	kafkaCheckName = "kafka"

	defaultPollInterval   = time.Second
	defaultBatchSize      = 100
	defaultKafkaBrokers   = "localhost:9092"
	defaultDatabaseURL    = "postgres://outbox_publisher:lab_password@localhost:5432/payments_db?sslmode=disable"
	defaultHTTPAddr       = ":8081"
	defaultPublishTimeout = 5 * time.Second
)

type config struct {
	PollInterval   time.Duration
	BatchSize      int
	KafkaBrokers   []string
	DatabaseURL    string
	HTTPAddr       string
	PublishTimeout time.Duration
}

func loadConfig() (config, error) {
	pollInterval, err := envDurationOrDefault("OUTBOX_PUBLISHER_POLL_INTERVAL", defaultPollInterval)
	if err != nil {
		return config{}, err
	}
	batchSize, err := envIntOrDefault("OUTBOX_PUBLISHER_BATCH_SIZE", defaultBatchSize)
	if err != nil {
		return config{}, err
	}
	publishTimeout, err := envDurationOrDefault("OUTBOX_PUBLISHER_PUBLISH_TIMEOUT", defaultPublishTimeout)
	if err != nil {
		return config{}, err
	}

	cfg := config{
		PollInterval:   pollInterval,
		BatchSize:      batchSize,
		KafkaBrokers:   parseBrokers(envOrDefault("OUTBOX_PUBLISHER_KAFKA_BROKERS", defaultKafkaBrokers)),
		DatabaseURL:    envOrDefault("OUTBOX_PUBLISHER_DATABASE_URL", defaultDatabaseURL),
		HTTPAddr:       envOrDefault("OUTBOX_PUBLISHER_HTTP_ADDR", defaultHTTPAddr),
		PublishTimeout: publishTimeout,
	}
	if err := validateConfig(cfg); err != nil {
		return config{}, err
	}
	return cfg, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envDurationOrDefault(name string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid duration", name)
	}
	return value, nil
}

func envIntOrDefault(name string, fallback int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("%s must be a valid integer", name)
	}
	return value, nil
}

func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		broker := strings.TrimSpace(part)
		if broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}

func validateConfig(cfg config) error {
	if cfg.PollInterval <= 0 {
		return errors.New("OUTBOX_PUBLISHER_POLL_INTERVAL must be positive")
	}
	if cfg.BatchSize <= 0 {
		return errors.New("OUTBOX_PUBLISHER_BATCH_SIZE must be positive")
	}
	if cfg.PublishTimeout <= 0 {
		return errors.New("OUTBOX_PUBLISHER_PUBLISH_TIMEOUT must be positive")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return errors.New("OUTBOX_PUBLISHER_KAFKA_BROKERS must contain at least one broker")
	}
	for _, broker := range cfg.KafkaBrokers {
		if err := validateHostPort(broker, "OUTBOX_PUBLISHER_KAFKA_BROKERS"); err != nil {
			return err
		}
	}
	if strings.TrimSpace(cfg.HTTPAddr) == "" {
		return errors.New("OUTBOX_PUBLISHER_HTTP_ADDR must not be empty")
	}
	if err := validateHostPort(cfg.HTTPAddr, "OUTBOX_PUBLISHER_HTTP_ADDR"); err != nil {
		return err
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return errors.New("OUTBOX_PUBLISHER_DATABASE_URL must not be empty")
	}
	if _, err := pgx.ParseConfig(cfg.DatabaseURL); err != nil {
		return errors.New("OUTBOX_PUBLISHER_DATABASE_URL is invalid")
	}
	return nil
}

func validateHostPort(value, name string) error {
	_, port, err := net.SplitHostPort(value)
	if err != nil {
		return fmt.Errorf("%s must be in host:port form", name)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return fmt.Errorf("%s must include a valid port", name)
	}
	return nil
}
