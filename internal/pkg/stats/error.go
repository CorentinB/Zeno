package stats

import "errors"

var (
	// ErrStatsNotInitialized is returned when the stats package is not initialized
	ErrStatsNotInitialized = errors.New("stats not initialized")
	// ErrStatsAlreadyInitialized is returned when the stats package is already initialized
	ErrStatsAlreadyInitialized = errors.New("stats already initialized")
)
