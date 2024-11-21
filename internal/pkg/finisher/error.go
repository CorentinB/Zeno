package finisher

import "errors"

var (
	// ErrFinisherAlreadyInitialized is the error returned when the finisher is already initialized
	ErrFinisherAlreadyInitialized = errors.New("finisher already initialized")
	// ErrFinisherNotInitialized is the error returned when the finisher is not initialized
	ErrFinisherNotInitialized = errors.New("finisher not initialized")
)
