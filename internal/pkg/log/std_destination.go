package log

import (
	"fmt"
	"log/slog"
	"os"
)

// StdoutDestination logs to stdout
type StdoutDestination struct {
	level slog.Level
}

func (d *StdoutDestination) Enabled() bool {
	return true
}

func (d *StdoutDestination) Level() slog.Level {
	return d.level
}

func (d *StdoutDestination) Write(entry *logEntry) {
	if entry.level < config.StderrLevel || !config.StderrEnabled {
		fmt.Println(formatLogEntry(entry))
	}
}

func (d *StdoutDestination) Close() {}

// StderrDestination logs to stderr
type StderrDestination struct {
	level slog.Level
}

func (d *StderrDestination) Enabled() bool {
	return true
}

func (d *StderrDestination) Level() slog.Level {
	return d.level
}

func (d *StderrDestination) Write(entry *logEntry) {
	if entry.level >= config.StderrLevel {
		fmt.Fprintln(os.Stderr, formatLogEntry(entry))
	}
}

func (d *StderrDestination) Close() {}
