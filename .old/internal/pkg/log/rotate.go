package log

import (
	"fmt"
	"log/slog"
	"time"
)

type rotatableHandler interface {
	slog.Handler
	Rotate() error
	NextRotation() time.Time
}

func (l *Logger) startRotation() {
	l.stopRotation = make(chan struct{})
	go func() {
		for {
			nextRotation := l.nextRotation()
			select {
			case <-time.After(time.Until(nextRotation)):
				l.rotate()
			case <-l.stopRotation:
				return
			}
		}
	}()
}

// nextRotation returns the earliest next rotation time
// of all rotatable handlers
func (l *Logger) nextRotation() time.Time {
	l.Lock()
	defer l.Unlock()

	var earliest time.Time
	for _, h := range l.handler.handlers {
		if rh, ok := h.(rotatableHandler); ok {
			next := rh.NextRotation()
			if earliest.IsZero() || next.Before(earliest) {
				earliest = next
			}
		}
	}
	return earliest
}

// rotate rotates
func (l *Logger) rotate() {
	l.Lock()
	defer l.Unlock()

	now := time.Now()
	for _, h := range l.handler.handlers {
		if rh, ok := h.(rotatableHandler); ok {
			if now.After(rh.NextRotation()) || now.Equal(rh.NextRotation()) {
				if err := rh.Rotate(); err != nil {
					fmt.Printf("Error rotating handler: %v\n", err)
				}
			}
		}
	}
}

// StopRotation stops the rotation goroutine
func (l *Logger) StopRotation() {
	close(l.stopRotation)
}
