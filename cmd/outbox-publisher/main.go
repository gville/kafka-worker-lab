// Command outbox-publisher publishes pending payment events from the outbox.
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gville/kafka-worker-lab/internal/health"
	"github.com/gville/kafka-worker-lab/internal/kafka"
	"github.com/gville/kafka-worker-lab/internal/logging"
	_ "github.com/jackc/pgx/v5/stdlib"
)

var shutdownTimeout = 5 * time.Second

type closer interface {
	Close()
}

type dbCloser interface {
	Close() error
}

type httpShutdowner interface {
	Shutdown(context.Context) error
	Close() error
}

type shutdownDeps struct {
	server   httpShutdowner
	cancel   context.CancelFunc
	loopDone <-chan struct{}
	producer closer
	db       dbCloser
}

type serveResult struct {
	ch   <-chan error
	err  error
	have bool
}

func (s serveResult) wait() error {
	if s.have {
		return s.err
	}
	return <-s.ch
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
		logger.Error("outbox-publisher stopped with error", "error", err)
		os.Exit(1)
	}
}

func newHealthHandlers(checks []health.Dependency) (http.Handler, http.Handler) {
	liveness := health.NewHandler(health.Config{})
	readiness := health.NewHandler(health.Config{Dependencies: checks})
	return liveness, readiness
}

func readinessChecks(logger *slog.Logger, postgres, kafkaCheck health.Check) []health.Dependency {
	return []health.Dependency{
		{Name: postgresCheck, Check: loggedCheck(logger, postgresCheck, postgres)},
		{Name: kafkaCheckName, Check: loggedCheck(logger, kafkaCheckName, kafkaCheck)},
	}
}

func loggedCheck(logger *slog.Logger, name string, check health.Check) health.Check {
	return func(ctx context.Context) error {
		if check == nil {
			err := errors.New("readiness check is not configured")
			logger.Error("readiness check failed", "dependency", name, "error", err)
			return err
		}
		if err := check(ctx); err != nil {
			logger.Error("readiness check failed", "dependency", name, "error", err)
			return err
		}
		return nil
	}
}

func newRouter(liveness, readiness http.Handler) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/health", liveness)
	mux.Handle("/ready", readiness)
	return mux
}

func run(cfg config, logger *slog.Logger) error {
	db, err := sql.Open("pgx", cfg.DatabaseURL)
	if err != nil {
		return errors.New("could not initialize postgres connection")
	}

	producer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:  cfg.KafkaBrokers,
		ClientID: kafkaClientID,
	})
	if err != nil {
		_ = db.Close()
		return fmt.Errorf("could not initialize Kafka producer: %w", err)
	}

	liveness, readiness := newHealthHandlers(readinessChecks(logger, db.PingContext, producer.Check))
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: newRouter(liveness, readiness),
	}

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	workCtx, cancelWork := context.WithCancel(signalCtx)
	defer cancelWork()

	store := newOutboxStore(db)
	worker := &publisher{
		store:          store,
		producer:       producer,
		logger:         logger,
		batchSize:      cfg.BatchSize,
		pollInterval:   cfg.PollInterval,
		publishTimeout: cfg.PublishTimeout,
	}

	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		if err := worker.Run(workCtx); err != nil && workCtx.Err() == nil {
			logger.Error("publisher stopped with error", "error", err)
		}
	}()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.ListenAndServe()
	}()

	logger.Info("outbox-publisher starting",
		"httpAddr", cfg.HTTPAddr,
		"pollInterval", cfg.PollInterval,
		"batchSize", cfg.BatchSize,
		"publishTimeout", cfg.PublishTimeout,
		"topic", paymentsTopic)

	select {
	case err := <-serverErr:
		return shutdown(logger, shutdownDeps{
			server:   server,
			cancel:   cancelWork,
			loopDone: loopDone,
			producer: producer,
			db:       db,
		}, serveResult{err: err, have: true})
	case <-signalCtx.Done():
		logger.Info("outbox-publisher shutdown requested")
		return shutdown(logger, shutdownDeps{
			server:   server,
			cancel:   cancelWork,
			loopDone: loopDone,
			producer: producer,
			db:       db,
		}, serveResult{ch: serverErr})
	}
}

func stopHTTP(logger *slog.Logger, server httpShutdowner) (shutdownErr, closeErr error) {
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	shutdownErr = server.Shutdown(shutdownCtx)
	if shutdownErr == nil {
		return nil, nil
	}

	logger.Error("graceful HTTP shutdown failed; forcing close", "error", shutdownErr)
	closeErr = server.Close()
	if closeErr != nil {
		logger.Error("http server close failed", "error", closeErr)
	}
	return shutdownErr, closeErr
}

func stopPublisherAndDependencies(deps shutdownDeps) error {
	deps.cancel()
	<-deps.loopDone
	deps.producer.Close()
	return deps.db.Close()
}

func joinShutdownErrors(logger *slog.Logger, shutdownErr, closeErr, serveErr, dbErr error) error {
	var errs []error

	if shutdownErr != nil {
		errs = append(errs, fmt.Errorf("http server shutdown failed: %w", shutdownErr))
	}
	if closeErr != nil {
		errs = append(errs, fmt.Errorf("http server close failed: %w", closeErr))
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Error("http server stopped unexpectedly", "error", serveErr)
		errs = append(errs, fmt.Errorf("http server stopped unexpectedly: %w", serveErr))
	}
	if dbErr != nil {
		logger.Error("postgres close failed", "error", dbErr)
		errs = append(errs, fmt.Errorf("could not close postgres connection: %w", dbErr))
	}

	return errors.Join(errs...)
}

func shutdown(logger *slog.Logger, deps shutdownDeps, serve serveResult) error {
	shutdownErr, closeErr := stopHTTP(logger, deps.server)
	dbErr := stopPublisherAndDependencies(deps)
	serveErr := serve.wait()

	err := joinShutdownErrors(logger, shutdownErr, closeErr, serveErr, dbErr)
	if err == nil {
		logger.Info("outbox-publisher stopped")
	}
	return err
}
