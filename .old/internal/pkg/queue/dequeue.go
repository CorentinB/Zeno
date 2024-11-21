package queue

import (
	"fmt"

	"github.com/internetarchive/Zeno/internal/pkg/queue/index"
)

// Dequeue removes and returns the next item from the queue
// It blocks until an item is available
func (q *PersistentGroupedQueue) Dequeue() (*Item, error) {
	return q.dequeueOp()
}

func (q *PersistentGroupedQueue) dequeueNoCommit() (*Item, error) {
	if !q.CanDequeue() {
		return nil, ErrDequeueClosed
	}

	if q.HandoverOpen.Get() {
		if item, ok := q.handover.tryGet(); ok && item != nil {
			q.handoverCount.Add(1)
			return item.item, nil
		}
	}

	var (
		position = uint64(0)
		size     = uint64(0)
		errPop   error
	)

	host, err := q.getNextHost()
	if err != nil {
		if err == ErrQueueEmpty || err == ErrDequeueClosed {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get next host: %w", err)
	}

	_, _, position, size, errPop = q.index.Pop(host)
	if errPop != nil && errPop != index.ErrHostEmpty {
		if errPop == index.ErrHostNotFound {
			return q.Dequeue() // Try again with another host, this one might be empty due to the non-blocking nature of getNextHost
		}
		return nil, fmt.Errorf("failed to pop item from host %s: %w", host, errPop)
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if position == 0 && size == 0 {
		q.Empty.Set(true)
		return nil, ErrQueueEmpty
	}

	// Read and unmarshal the item
	itemBytes, err := q.ReadItemAt(position, size)
	if err != nil {
		return nil, fmt.Errorf("failed to read item at position %d: %w", position, err)
	}

	item, err := decodeProtoItem(itemBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal item: %w", err)
	}

	q.updateDequeueStats(item.URL.Host)

	return item, nil
}

func (q *PersistentGroupedQueue) dequeueCommitted() (*Item, error) {
	if !q.CanDequeue() {
		return nil, ErrDequeueClosed
	}

	if q.HandoverOpen.Get() {
		if item, ok := q.handover.tryGet(); ok && item != nil {
			q.handoverCount.Add(1)
			return item.item, nil
		}
	}

	var (
		commit   = uint64(0)
		position = uint64(0)
		size     = uint64(0)
		errPop   error
	)

	host, err := q.getNextHost()
	if err != nil {
		if err == ErrQueueEmpty || err == ErrDequeueClosed {
			return nil, err
		}
		return nil, fmt.Errorf("failed to get next host: %w", err)
	}

	commit, _, position, size, errPop = q.index.Pop(host)
	if errPop != nil && errPop != index.ErrHostEmpty {
		if errPop == index.ErrHostNotFound {
			return q.Dequeue() // Try again with another host, this one might be empty due to the non-blocking nature of getNextHost
		}
		return nil, fmt.Errorf("failed to pop item from host %s: %w", host, errPop)
	}

	q.index.AwaitWALCommitted(commit)

	q.mutex.Lock()
	defer q.mutex.Unlock()

	if position == 0 && size == 0 {
		q.Empty.Set(true)
		return nil, ErrQueueEmpty
	}

	// Read and unmarshal the item
	itemBytes, err := q.ReadItemAt(position, size)
	if err != nil {
		return nil, fmt.Errorf("failed to read item at position %d: %w", position, err)
	}

	item, err := decodeProtoItem(itemBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal item: %w", err)
	}

	q.updateDequeueStats(item.URL.Host)

	return item, nil
}

// getNextHost returns the next host to dequeue from
func (q *PersistentGroupedQueue) getNextHost() (string, error) {
	if q.Empty.Get() && q.CanDequeue() {
		return "", ErrQueueEmpty
	}

	// If the queue is closed or finishing, we return an error
	if !q.CanDequeue() {
		return "", ErrDequeueClosed
	}

	// Get a copy of the hosts
	hosts := q.index.GetHosts()

	// If there are no hosts, we wait for one to be added
	if len(hosts) == 0 {
		q.Empty.Set(true)
		return q.getNextHost()
	}

	// Magic operating : get the next host to dequeue from
	i := q.currentHost.Load() % uint64(len(hosts))
	q.currentHost.Add(1)
	return hosts[i], nil
}
