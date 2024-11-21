package log

import (
	"context"
	"fmt"
	"log/slog"
)

// multiHandler implements slog.Handler interface for multiple outputs
type multiHandler struct {
	handlers []slog.Handler
}

// Enabled checks if any of the underlying handlers are enabled for a given log level.
// It's used internally to determine if a log message should be processed by a given handler
func (h *multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, handler := range h.handlers {
		if handler.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

// Handle is responsible for passing the log record to all underlying handlers.
// It's called internally when a log message needs to be written.
func (h *multiHandler) Handle(ctx context.Context, r slog.Record) error {
	var errs []error
	for _, handler := range h.handlers {
		if err := handler.Handle(ctx, r); err != nil {
			errs = append(errs, fmt.Errorf("handler error: %w", err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("multiple handler errors: %v", errs)
	}
	return nil
}

// WithAttrs creates a new handler with additional attributes.
// It's used internally when the logger is asked to include additional context with all subsequent log messages.
func (h *multiHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithAttrs(attrs)
	}
	return &multiHandler{handlers: handlers}
}

// WithGroups creates a new handler with a new group added to the attribute grouping hierarchy.
// It's used internally when the logger is asked to group a set of attributes together.
func (h *multiHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, handler := range h.handlers {
		handlers[i] = handler.WithGroup(name)
	}
	return &multiHandler{handlers: handlers}
}
