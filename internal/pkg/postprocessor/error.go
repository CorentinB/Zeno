package postprocessor

import "errors"

var (
	// ErrPostprocessorAlreadyInitialized is the error returned when the postprocessor is already initialized
	ErrPostprocessorAlreadyInitialized = errors.New("postprocessor already initialized")
)
