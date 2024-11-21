package log

import (
	"fmt"
	"os"
)

// WatchErrors watches for errors in the logger and prints them to stderr.
func (l *Logger) WatchErrors() {
	go func() {
		errChan := l.Errors()
		for {
			select {
			case <-l.stopErrorLog:
				return
			case err := <-errChan:
				fmt.Fprintf(os.Stderr, "Logging error: %v\n", err)
			}
		}
	}()
}

// StopErrorLog stops the error logger.
func (l *Logger) StopErrorLog() {
	close(l.stopErrorLog)
}
