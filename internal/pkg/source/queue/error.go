package queue

import "errors"

var (
	// ErrQueueFull is returned when the queue is full
	ErrQueueFull = errors.New("queue is full")

	// ErrQueueEmpty is returned when the queue is empty
	ErrQueueEmpty = errors.New("queue is empty")

	// ErrQueueClosed is returned when the queue is closed
	ErrQueueClosed = errors.New("queue is closed")

	// ErrQueueTimeout is returned when the queue operation times out
	ErrQueueTimeout = errors.New("queue operation timed out")

	// ErrQueueAlreadyClosed is returned when the queue is already closed
	ErrQueueAlreadyClosed = errors.New("queue is already closed")

	// ErrDequeueClosed is returned when the dequeue operation is called on a closed or empty queue
	ErrDequeueClosed = errors.New("dequeue operation called on a closed or empty queue")

	// ErrCommitValueNotReceived is returned when a commit value is not received when it should have been
	ErrCommitValueNotReceived = errors.New("commit value not received when it should have been")
)
