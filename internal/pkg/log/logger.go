// logger.go
package log

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

type logEntry struct {
	timestamp time.Time
	level     slog.Level
	msg       string
	args      []any
}

func setupLogger() {
	// Initialize the log queue
	logQueue = make(chan *logEntry, 10000)

	// Create a cancellable context
	var ctx context.Context
	ctx, cancelFunc = context.WithCancel(context.Background())

	// Start the log processing goroutine
	go processLogQueue(ctx)
}

func processLogQueue(ctx context.Context) {
	wg.Add(1)
	defer wg.Done()

	// Initialize log destinations
	destinations := initDestinations()

	for {
		select {
		case entry := <-logQueue:
			// Process the log entry
			for _, dest := range destinations {
				if dest.Enabled() && entry.level >= dest.Level() {
					dest.Write(entry)
				}
			}
		case <-ctx.Done():
			// Drain the log queue before exiting
			for len(logQueue) > 0 {
				entry := <-logQueue
				for _, dest := range destinations {
					if dest.Enabled() && entry.level >= dest.Level() {
						dest.Write(entry)
					}
				}
			}
			// Close destinations
			for _, dest := range destinations {
				dest.Close()
			}
			return
		}
	}
}

// Helper function to format args
func formatArgs(args []any) string {
	var sb strings.Builder

	for i := 0; i < len(args); i += 2 {
		if i+1 < len(args) {
			sb.WriteString(fmt.Sprintf("%v=%v", args[i], args[i+1]))
		} else {
			sb.WriteString(fmt.Sprintf("%v", args[i]))
		}
		if i+2 < len(args) {
			sb.WriteString("\t")
		}
	}

	return sb.String()
}

// Helper function to format log entries
func formatLogEntry(entry *logEntry) string {
	return fmt.Sprintf("%s [%s] %s\t%s", entry.timestamp.Format(time.RFC3339), entry.level.String(), entry.msg, formatArgs(entry.args))
}
