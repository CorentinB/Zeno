package archiver

import "errors"

var (
	// ErrArchiverAlreadyInitialized is the error returned when the preprocess is already initialized
	ErrArchiverAlreadyInitialized = errors.New("archiver already initialized")
)
