package index

import "errors"

var (
	// ErrHostNotFound is returned when the host is not found in the index
	ErrHostNotFound = errors.New("host not found")

	// ErrHostEmpty is returned when the host is empty
	ErrHostEmpty = errors.New("host cannot be empty")

	// ErrIDEmpty is returned when the given ID is empty
	ErrIDEmpty = errors.New("id cannot be empty")

	// ErrNoWALEntriesReplayed is returned when no WAL entries were replayed
	ErrNoWALEntriesReplayed = errors.New("no WAL entries replayed")
)
