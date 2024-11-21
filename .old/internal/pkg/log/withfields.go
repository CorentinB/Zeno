package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// WithFields returns a new fielded logger with the given fields.
// The fields are key-value pairs that will be included in the given logger.
//
// This method returns a log FieldedLogger, which can be used to log a message with the specified fields.
//
// Parameters:
//   - fields: A map of key-value pairs to be included in the fielded logger
//
// Returns:
//   - FieldedLogger: A logger with fields that can be used to log messages with the given fields
//
// Example:
//
//	logger := log.Default()
//	logger.WithFields(map[string]interface{}{
//	    "user_id": 12345,
//	    "ip": "192.168.1.1",
//	}).Info("User logged in")
func (l *Logger) WithFields(fields map[string]interface{}) *FieldedLogger {
	attrs := make([]slog.Attr, 0, len(fields))
	for k, v := range fields {
		attrs = append(attrs, slog.Any(k, v))
	}
	return &FieldedLogger{logger: l, attrs: attrs}
}

// FieldedLogger is a logger with fields.
type FieldedLogger struct {
	logger *Logger
	attrs  []slog.Attr
}

func (e *FieldedLogger) log(ctx context.Context, level slog.Level, msg string, args ...any) {
	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			e.attrs = append(e.attrs, slog.Any(fmt.Sprint(args[i]), args[i+1]))
		}
	}
	r := slog.NewRecord(time.Now(), level, msg, 0)
	r.AddAttrs(e.attrs...)
	_ = e.logger.handler.Handle(ctx, r)
}

// Info logs a message at Info level with the fields specified in WithFields.
func (e *FieldedLogger) Info(msg string, args ...any) {
	e.log(context.Background(), slog.LevelInfo, msg, args...)
}

// Warn logs a message at Warn level with the fields specified in WithFields.
func (e *FieldedLogger) Warn(msg string, args ...any) {
	e.log(context.Background(), slog.LevelWarn, msg, args...)
}

// Error logs a message at Error level with the fields specified in WithFields.
func (e *FieldedLogger) Error(msg string, args ...any) {
	e.log(context.Background(), slog.LevelError, msg, args...)
}

// Debug logs a message at Debug level with the fields specified in WithFields.
func (e *FieldedLogger) Debug(msg string, args ...any) {
	e.log(context.Background(), slog.LevelDebug, msg, args...)
}

// Fatal logs a message at Fatal level with the fields specified in WithFields, then calls os.Exit(1).
func (e *FieldedLogger) Fatal(msg string, args ...any) {
	e.log(context.Background(), slog.LevelError, msg, args...)
	os.Exit(1)
}
