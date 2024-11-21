package hq

import "errors"

var (
	// ErrHQAlreadyInitialized is the error returned when the postprocessor is already initialized
	ErrHQAlreadyInitialized = errors.New("hq client already initialized")
)
