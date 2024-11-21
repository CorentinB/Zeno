package reactor

import "errors"

var (
	// ErrReactorAlreadyInitialized is the error returned when the reactor is already initialized
	ErrReactorAlreadyInitialized = errors.New("reactor already initialized")
	// ErrReactorNotInitialized is the error returned when the reactor is not initialized
	ErrReactorNotInitialized = errors.New("reactor not initialized")
	// ErrReactorShuttingDown is the error returned when the reactor is shutting down
	ErrReactorShuttingDown = errors.New("reactor shutting down")
	// ErrReactorFrozen is the error returned when the reactor is frozen
	ErrReactorFrozen = errors.New("reactor frozen")

	// ErrFeedbackItemNotPresent is the error returned when an item was sent to the feedback channel but not found in the state table
	ErrFeedbackItemNotPresent = errors.New("feedback item not present in state table")
	// ErrFinisehdItemNotFound is the error returned when an item been marked as finished but not found in the state table
	ErrFinisehdItemNotFound = errors.New("markAsFinished item not present in state table")
)
