package queue

func (q *PersistentGroupedQueue) CanEnqueue() bool {
	return !q.closed.Get()
}

func (q *PersistentGroupedQueue) CanDequeue() bool {
	return !q.closed.Get() && !q.finishing.Get()
}
