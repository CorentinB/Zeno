package log

import (
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"
)

// FileDestination logs to a file with rotation
type FileDestination struct {
	level     slog.Level
	config    *LogfileConfig
	file      *os.File
	mu        sync.Mutex
	ticker    *time.Ticker
	closeChan chan struct{}
}

func NewFileDestination(cfg *LogfileConfig) *FileDestination {
	fd := &FileDestination{
		level:     cfg.Level,
		config:    cfg,
		closeChan: make(chan struct{}),
	}

	fd.rotateFile()
	if config.RotateLogFile && config.RotatePeriod > 0 {
		fd.ticker = time.NewTicker(config.RotatePeriod)
		go fd.rotationWorker()
	}

	return fd
}

func (d *FileDestination) Enabled() bool {
	return true
}

func (d *FileDestination) Level() slog.Level {
	return d.level
}

func (d *FileDestination) Write(entry *logEntry) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		fmt.Fprintln(d.file, formatLogEntry(entry))
	}
}

func (d *FileDestination) Close() {
	if d.ticker != nil {
		d.ticker.Stop()
	}
	close(d.closeChan)
	d.mu.Lock()
	if d.file != nil {
		d.file.Close()
	}
	d.mu.Unlock()
}

func (d *FileDestination) rotateFile() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file != nil {
		d.file.Close()
	}
	filename := fmt.Sprintf("%s/%s-%s.log", d.config.Dir, d.config.Prefix, time.Now().Format("2006.01.02T15-04"))
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		// Handle error (for simplicity, we'll just ignore it here)
		return
	}
	d.file = file
}

func (d *FileDestination) rotationWorker() {
	for {
		select {
		case <-d.ticker.C:
			d.rotateFile()
		case <-d.closeChan:
			return
		}
	}
}
