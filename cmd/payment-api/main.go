// Command payment-api serves the Payment API scaffold for the local lab.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gville/kafka-worker-lab/internal/health"
	"github.com/gville/kafka-worker-lab/internal/logging"
	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const (
	serviceName        = "payment-api"
	dependencyName     = "payments_db"
	defaultHTTPAddr    = ":8080"
	defaultDatabaseURL = "postgres://payment_api:lab_password@localhost:5432/payments_db?sslmode=disable"
)

var shutdownTimeout = 5 * time.Second

type config struct {
	HTTPAddr    string
	DatabaseURL string
}

func main() {
	cfg, cfgErr := loadConfig()
	logger := logging.New(logging.Config{
		Service: serviceName,
		Level:   slog.LevelInfo,
		Writer:  os.Stdout,
	})
	if cfgErr != nil {
		logger.Error("configuration error", "error", cfgErr)
		os.Exit(1)
	}

	if err := run(cfg, logger); err != nil {
		logger.Error("payment-api stopped with error", "error", err)
		os.Exit(1)
	}
}

func loadConfig() (config, error) {
	cfg := config{
		HTTPAddr:    envOrDefault("PAYMENT_API_HTTP_ADDR", defaultHTTPAddr),
		DatabaseURL: envOrDefault("PAYMENT_API_DATABASE_URL", defaultDatabaseURL),
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

func validateConfig(cfg config) error {
	if strings.TrimSpace(cfg.HTTPAddr) == "" {
		return errors.New("PAYMENT_API_HTTP_ADDR must not be empty")
	}
	_, port, err := net.SplitHostPort(cfg.HTTPAddr)
	if err != nil {
		return errors.New("PAYMENT_API_HTTP_ADDR must be in host:port form")
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("PAYMENT_API_HTTP_ADDR must include a valid port")
	}
	if strings.TrimSpace(cfg.DatabaseURL) == "" {
		return errors.New("PAYMENT_API_DATABASE_URL must not be empty")
	}
	if _, err := pgx.ParseConfig(cfg.DatabaseURL); err != nil {
		return errors.New("PAYMENT_API_DATABASE_URL is invalid")
	}
	return nil
}

func newHealthHandler(check health.Check) http.Handler {
	return health.NewHandler(health.Config{
		Dependencies: []health.Dependency{{Name: dependencyName, Check: check}},
	})
}

func newRouter(healthHandler, paymentsHandler http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/health", healthHandler)
	mux.Handle("/ready", healthHandler)
	mux.Handle("POST /payments", paymentsHandler)
	return mux
}

func run(cfg config, logger *slog.Logger) error {
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return errors.New("could not initialize payments_db connection")
	}

	healthHandler := newHealthHandler(db.PingContext)
	store := newPostgresPaymentStore(db)
	paymentsHandler := newPaymentsHandler(store, logger)

	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(healthHandler, paymentsHandler),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger.Info("payment-api starting", "httpAddr", cfg.HTTPAddr)

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			_ = db.Close()
			return err
		}
		if closeErr := db.Close(); closeErr != nil {
			return errors.New("could not close payments_db connection")
		}
		return nil
	case <-ctx.Done():
		logger.Info("payment-api shutdown requested")
		return shutdown(logger, server, serverErr, db)
	}
}

func shutdown(logger *slog.Logger, server *http.Server, serverErr <-chan error, db *sql.DB) error {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	shutdownErr := server.Shutdown(shutdownCtx)
	if shutdownErr != nil {
		logger.Error("graceful shutdown failed; forcing close", "error", shutdownErr)
		if closeErr := server.Close(); closeErr != nil {
			logger.Error("http server close failed", "error", closeErr)
		}
	}

	serveErr := <-serverErr
	closeErr := db.Close()

	if shutdownErr != nil {
		if closeErr != nil {
			logger.Error("payments_db close failed during shutdown", "error", closeErr)
		}
		return fmt.Errorf("http server shutdown failed: %w", shutdownErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Error("http server stopped unexpectedly", "error", serveErr)
		return fmt.Errorf("http server stopped unexpectedly: %w", serveErr)
	}
	if closeErr != nil {
		logger.Error("payments_db close failed", "error", closeErr)
		return fmt.Errorf("could not close payments_db connection: %w", closeErr)
	}

	logger.Info("payment-api stopped")
	return nil
}
