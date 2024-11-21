package log

import (
	"context"
	"io"
	"log/slog"
)

// logWriter implements io.Writer interface for compatibility with Gin
type logWriter struct {
	logger *Logger
	level  slog.Level
}

// Write implements io.Writer interface.
// It writes the log message at the specified level.
func (w *logWriter) Write(p []byte) (n int, err error) {
	w.logger.slogger.Log(context.Background(), w.level, string(p))
	return len(p), nil
}

// Writer returns an io.Writer that logs at the specified level.
// This can be used to integrate with Gin's logging system.
func (l *Logger) Writer(level slog.Level) io.Writer {
	return &logWriter{
		logger: l,
		level:  level,
	}
}
