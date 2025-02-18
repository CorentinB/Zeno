package queue

// CanEnqueue returns true if the queue is open and can accept new items.
func (q *PersistentGroupedQueue) CanEnqueue() bool {
	return !q.closed.Get()
}

// CanDequeue returns true if the queue is open and can provide items.
func (q *PersistentGroupedQueue) CanDequeue() bool {
	return !q.closed.Get() && !q.finishing.Get()
}
