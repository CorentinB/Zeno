// Package log provides a custom logging solution with multi-output support
// and log rotation for file output.
// -----------------------------------------------------------------------------
// When Logger.{Debug, Info, Warn, Error, Fatal} is called, the log message is
// passed to all underlying handlers represented by Logger.handler
// Then multiHandler.Handle is called to pass the log message to all underlying handlers.
// -----------------------------------------------------------------------------
// The rotation mechanism works by locking the logger, checking if it's time to rotate,
// and then calling the Rotate method on all rotatable handlers.
package log

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
)

var (
	isLoggerInit *atomic.Bool
	storedLogger *Logger
	once         sync.Once
)

// Logger wraps slog.Logger to provide multi-output functionality
type Logger struct {
	sync.Mutex
	handler      *multiHandler
	slogger      *slog.Logger
	stopRotation chan struct{}
	stopErrorLog chan struct{}
	errorChan    chan error
}

// Config holds the configuration for the logger
type Config struct {
	FileConfig               *LogfileConfig
	FileLevel                slog.Level
	StdoutEnabled            bool
	StdoutLevel              slog.Level
	RotateLogFile            bool
	ElasticsearchConfig      *ElasticsearchConfig
	RotateElasticSearchIndex bool
	isDefault                bool
}

// New creates a new Logger instance with the given configuration.
// It sets up handlers for stdout (text format) and file output (JSON format) if specified.
// If FileOutput is empty, only stdout logging will be enabled.
// Only the first call to New will store the logger to be reused. Subsequent calls will return a new logger instance.
// Only the first call to New will rotate the logs destinations.
// Please refrain from calling New multiple times in the same program.
//
// Parameters:
//   - cfg: Config struct containing logger configuration options
//
// Returns:
//   - *Logger: A new Logger instance
//   - error: An error if there was a problem creating the logger (e.g., unable to open log file)
func New(cfg Config) (*Logger, error) {
	var handlers []slog.Handler

	// Create stdout handler
	if cfg.StdoutEnabled {
		stdoutHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: cfg.StdoutLevel,
		})
		handlers = append(handlers, stdoutHandler)
	}

	// Create file handler if FileOutput is specified
	if cfg.FileConfig != nil {
		// Create directories if they don't exist
		err := os.MkdirAll(filepath.Dir(cfg.FileConfig.Filename()), 0755)
		if err != nil {
			return nil, err
		}

		// Open log file
		file, err := os.OpenFile(cfg.FileConfig.Filename(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		fileHandler := &fileHandler{
			Handler:          slog.NewJSONHandler(file, &slog.HandlerOptions{Level: cfg.FileLevel}),
			fileDescriptor:   file,
			rotationInterval: 6 * time.Hour,
			lastRotation:     time.Now(),
			logfileConfig:    cfg.FileConfig,
		}
		handlers = append(handlers, fileHandler)
	}

	// Create Elasticsearch handler if ElasticsearchConfig is specified
	if cfg.ElasticsearchConfig != nil {
		esClient, err := elasticsearch.NewClient(elasticsearch.Config{
			Addresses: cfg.ElasticsearchConfig.Addresses,
			Username:  cfg.ElasticsearchConfig.Username,
			Password:  cfg.ElasticsearchConfig.Password,
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
		}
		esHandler := &ElasticsearchHandler{
			client: esClient,
			index:  fmt.Sprintf("%s-%s", cfg.ElasticsearchConfig.IndexPrefix, time.Now().Format("2006.01.02")),
			level:  cfg.ElasticsearchConfig.Level,
			attrs:  []slog.Attr{},
			groups: []string{},
			config: cfg.ElasticsearchConfig,
		}
		if err := esHandler.createIndex(); err != nil {
			return nil, fmt.Errorf("failed to create Elasticsearch index: %w", err)
		}
		handlers = append(handlers, esHandler)
	}

	// Create multi-handler
	mh := &multiHandler{handlers: handlers}

	// Create slog.Logger
	slogger := slog.New(mh)

	logger := &Logger{
		handler:      mh,
		slogger:      slogger,
		errorChan:    make(chan error, 10),
		stopErrorLog: make(chan struct{}),
	}

	if !cfg.isDefault {
		once.Do(func() {
			isLoggerInit = new(atomic.Bool)
			storedLogger = logger
			isLoggerInit.CompareAndSwap(false, true)

			// Start rotation goroutine
			logger.startRotation()
		})
	}

	return logger, nil
}

// DefaultOrStored returns the default Logger instance or if already initialized, the logger created by first call to New().
// The default logger writes to both stdout (text format) and a file named "app.log" (JSON format).
// Both outputs are set to log messages at Info level and above.
// This function uses sync.Once to ensure that the default logger is only created once.
//
// Returns:
//   - *Logger: The default Logger instance
//   - bool: True if the logger was created by this function, false if the logger was already initialized
func DefaultOrStored() (*Logger, bool) {
	var created = false
	once.Do(func() {
		isLoggerInit = new(atomic.Bool)
		logger, err := New(Config{
			FileConfig:  &LogfileConfig{Dir: "jobs", Prefix: "zeno"},
			FileLevel:   slog.LevelInfo,
			StdoutLevel: slog.LevelInfo,
			isDefault:   true,
		})
		if err != nil {
			panic(err)
		}
		storedLogger = logger
		created = isLoggerInit.CompareAndSwap(false, true)
	})
	return storedLogger, created
}

// GetStoredLogger returns the logger created by the first call to New() or DefaultOrStored().
// If the logger has not been initialized, it will return nil.
func GetStoredLogger() *Logger {
	return storedLogger
}

// Errors returns a channel that will receive logging errors
func (l *Logger) Errors() <-chan error {
	return l.errorChan
}

func (l *Logger) log(level slog.Level, msg string, args ...any) {
	l.Lock()
	defer l.Unlock()

	// Create a new Record with the message and args
	r := slog.NewRecord(time.Now(), level, msg, 0)
	r.Add(args...)

	err := l.handler.Handle(context.Background(), r)
	if err != nil {
		select {
		case l.errorChan <- err:
		default:
			// If the error channel is full, log to stderr as a last resort
			fmt.Fprintf(os.Stderr, "Logging error: %v\n", err)
		}
	}
}

// Debug logs a message at Debug level.
// The first argument is the message to log, and subsequent arguments are key-value pairs
// that will be included in the log entry.
//
// Parameters:
//   - msg: The message to log
//   - args: Optional key-value pairs to include in the log entry
func (l *Logger) Debug(msg string, args ...any) {
	l.log(slog.LevelDebug, msg, args...)
}

// Info logs a message at Info level.
// The first argument is the message to log, and subsequent arguments are key-value pairs
// that will be included in the log entry.
//
// Parameters:
//   - msg: The message to log
//   - args: Optional key-value pairs to include in the log entry
func (l *Logger) Info(msg string, args ...any) {
	l.log(slog.LevelInfo, msg, args...)
}

// Warn logs a message at Warn level.
// The first argument is the message to log, and subsequent arguments are key-value pairs
// that will be included in the log entry.
//
// Parameters:
//   - msg: The message to log
//   - args: Optional key-value pairs to include in the log entry
func (l *Logger) Warn(msg string, args ...any) {
	l.log(slog.LevelWarn, msg, args...)
}

// Error logs a message at Error level.
// The first argument is the message to log, and subsequent arguments are key-value pairs
// that will be included in the log entry.
//
// Parameters:
//   - msg: The message to log
//   - args: Optional key-value pairs to include in the log entry
func (l *Logger) Error(msg string, args ...any) {
	l.log(slog.LevelError, msg, args...)
}

// Fatal logs a message at Error level and then calls os.Exit(1).
// The first argument is the message to log, and subsequent arguments are key-value pairs
// that will be included in the log entry.
//
// Parameters:
//   - msg: The message to log
//   - args: Optional key-value pairs to include in the log entry
func (l *Logger) Fatal(msg string, args ...any) {
	l.log(slog.LevelError, msg, args...)
	os.Exit(1)
}
