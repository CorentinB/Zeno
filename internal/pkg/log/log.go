// log.go
package log

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Global variables
var (
	config     *Config
	logQueue   chan *logEntry
	once       sync.Once
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
)

// Start initializes the logging package with the given configuration.
// If no configuration is provided, it uses the default configuration.
func Start(cfgs ...*Config) error {
	var done = false

	once.Do(func() {
		logQueue = make(chan *logEntry, 1000)
		if len(cfgs) > 0 && cfgs[0] != nil {
			config = cfgs[0]
		} else {
			config = defaultConfig()
		}
		setupLogger()
		done = true
	})

	if !done {
		return ErrLoggerAlreadyInitialized
	}

	return nil
}

// Public logging methods
func Debug(msg string, args ...any) {
	logWithLevel(slog.LevelDebug, msg, args...)
}

func Info(msg string, args ...any) {
	logWithLevel(slog.LevelInfo, msg, args...)
}

func Warn(msg string, args ...any) {
	logWithLevel(slog.LevelWarn, msg, args...)
}

func Error(msg string, args ...any) {
	logWithLevel(slog.LevelError, msg, args...)
}

// logWithLevel sends the log entry to the logQueue
func logWithLevel(level slog.Level, msg string, args ...any) {
	entry := &logEntry{
		timestamp: time.Now(),
		level:     level,
		msg:       msg,
		args:      args,
	}
	select {
	case logQueue <- entry:
	default:
		slog.Error("Log queue is full, dropping log entry from logger", "msg", msg, "args", args)
	}
}

// Stop gracefully shuts down the logging system
func Stop() {
	if cancelFunc != nil {
		cancelFunc()
	}
	wg.Wait()
	close(logQueue)
	once = sync.Once{}
}
