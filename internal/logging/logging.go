// Package logging provides narrow technical helpers for structured JSON logging.
package logging

import (
	"io"
	"log/slog"
	"os"
)

// Config contains the settings required to build a service logger.
type Config struct {
	Service string
	Level   slog.Level
	Writer  io.Writer
}

// New returns a structured JSON logger that writes to cfg.Writer, or to
// os.Stdout when no writer is provided.
func New(cfg Config) *slog.Logger {
	writer := cfg.Writer
	if writer == nil {
		writer = os.Stdout
	}

	opts := &slog.HandlerOptions{
		Level: cfg.Level,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				a.Key = "timestamp"
			}
			return a
		},
	}

	return slog.New(slog.NewJSONHandler(writer, opts)).With("service", cfg.Service)
}
