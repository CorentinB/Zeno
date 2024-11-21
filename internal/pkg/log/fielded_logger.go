package log

import (
	"log/slog"
)

// Field defines an interface for fields
type Fields map[string]interface{}

// FieldedLogger allows adding predefined fields to log entries
type FieldedLogger struct {
	fields *Fields
}

// NewFieldedLogger creates a new FieldedLogger with the given fields
func NewFieldedLogger(args *Fields) *FieldedLogger {
	return &FieldedLogger{
		fields: args,
	}
}

// FieldedLogger methods
func (fl *FieldedLogger) Debug(msg string, args ...any) {
	fl.logWithLevel(slog.LevelDebug, msg, args...)
}

func (fl *FieldedLogger) Info(msg string, args ...any) {
	fl.logWithLevel(slog.LevelInfo, msg, args...)
}

func (fl *FieldedLogger) Warn(msg string, args ...any) {
	fl.logWithLevel(slog.LevelWarn, msg, args...)
}

func (fl *FieldedLogger) Error(msg string, args ...any) {
	fl.logWithLevel(slog.LevelError, msg, args...)
}

func (fl *FieldedLogger) logWithLevel(level slog.Level, msg string, args ...any) {
	var combinedArgs []any

	if fl.fields != nil {
		for k, v := range *fl.fields {
			combinedArgs = append(combinedArgs, k)
			combinedArgs = append(combinedArgs, v)
		}
	}

	if len(args) > 0 {
		for _, arg := range args {
			combinedArgs = append(combinedArgs, arg)
		}
	}

	logWithLevel(level, msg, combinedArgs...)
}
