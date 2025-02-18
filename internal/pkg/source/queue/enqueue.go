package queue

import (
	"errors"
	"fmt"
	"io"
)

// Enqueue adds 1 or many items to the queue in a single operation.
// If multiple items are provided, the order in which they will be enqueued is not guaranteed.
func (q *PersistentGroupedQueue) Enqueue(items ...*Item) error {
	if items == nil {
		return errors.New("cannot enqueue nil item")
	}

	if !q.CanEnqueue() {
		return ErrQueueClosed
	}

	batchLen := len(items)
	if batchLen == 0 {
		return fmt.Errorf("cannot enqueue empty batch")
	}

	// Update empty status
	defer q.Empty.Set(false)

	var commit uint64
	var writtenCount int64

	// <- lock mutex
	err := func() error {
		q.mutex.Lock()
		defer q.mutex.Unlock()

		// Seek to end of file to get start position
		startPos, err := q.queueFile.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to seek to end of file: %s", err.Error())
		}

		for i := range items {
			// Encode item
			itemBytes, err := encodeItem(items[i])
			if err != nil {
				panic(fmt.Sprintf("failed to encode item: %s", err.Error()))
			}

			// Write item to disk
			commit, err = writeItemToFile(q, items[i], itemBytes, &startPos)
			if err != nil {
				return err
			}

			writtenCount++
		}

		return nil // success
	}()
	// below is outside of the mutex lock ->
	if err != nil {
		return err
	}

	if writtenCount != 0 && commit == 0 {
		return ErrCommitValueNotReceived
	} else if writtenCount != 0 && commit != 0 {
		q.index.AwaitWALCommitted(commit)
	}

	return nil
}

func writeItemToFile(q *PersistentGroupedQueue, item *Item, itemBytes []byte, startPos *int64) (commit uint64, err error) {
	// Write item to file
	written, err := q.queueFile.Write(itemBytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write item: %w", err)
	}

	// Update host index and order
	commit, err = q.index.Add(item.URL.Host, item.ID, uint64(*startPos), uint64(len(itemBytes)))
	if err != nil {
		return 0, fmt.Errorf("failed to update index: %w", err)
	}

	*startPos += int64(written)

	// Update stats
	q.updateEnqueueStats(item)

	return commit, nil
}
