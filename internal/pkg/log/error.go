package log

import "errors"

var (
	// ErrLoggerAlreadyInitialized is the error returned when the logger is already initialized
	ErrLoggerAlreadyInitialized = errors.New("logger already initialized")
)
