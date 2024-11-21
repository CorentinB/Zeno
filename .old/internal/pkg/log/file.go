package log

import (
	"fmt"
	"log/slog"
	"os"
	"time"
)

var (
	filenameFormat = "2006.01.02T15-04"
)

type fileHandler struct {
	slog.Handler
	fileDescriptor   *os.File
	rotationInterval time.Duration
	lastRotation     time.Time
	level            slog.Level
	logfileConfig    *LogfileConfig
}

// LogfileConfig represents the configuration for the log file output
type LogfileConfig struct {
	Dir    string
	Prefix string
}

func (h *fileHandler) Rotate() error {
	if h.fileDescriptor != nil {
		h.fileDescriptor.Close()
	}

	file, err := os.OpenFile(h.logfileConfig.Filename(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("failed to open new log file: %w", err)
	}

	h.fileDescriptor = file
	h.Handler = slog.NewJSONHandler(file, &slog.HandlerOptions{
		Level: h.level,
	})

	h.lastRotation = time.Now()
	return nil
}

func (h *fileHandler) NextRotation() time.Time {
	return h.lastRotation.Add(h.rotationInterval)
}

// Filename returns the computed filename of the log file with the current timestamp
func (s *LogfileConfig) Filename() string {
	return fmt.Sprintf("%s/%s.%s.log", s.Dir, s.Prefix, time.Now().Format(filenameFormat))
}
