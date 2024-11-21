package queue

import (
	"errors"
	"fmt"
	"io"
	"sync"
)

// BatchEnqueue adds 1 or many items to the queue in a single operation.
// If multiple items are provided, the order in which they will be enqueued is not guaranteed.
// It WILL be less efficient than Enqueue for single items.
func (q *PersistentGroupedQueue) BatchEnqueue(items ...*Item) error {
	return q.batchEnqueueOp(items...)
}

// Enqueue adds an item to the queue.
func (q *PersistentGroupedQueue) Enqueue(item *Item) error {
	return q.enqueueOp(item)
}

func (q *PersistentGroupedQueue) batchEnqueueNoCommit(items ...*Item) error {
	if !q.CanEnqueue() {
		return ErrQueueClosed
	}

	batchLen := len(items)
	if batchLen == 0 {
		return fmt.Errorf("cannot enqueue empty batch")
	}

	var (
		// Global
		isHandover bool

		// Handover
		failedHandoverItems = []*handoverEncodedItem{}

		// No Handover
		itemsChan = make(chan *handoverEncodedItem, batchLen)
		wg        = &sync.WaitGroup{}
	)

	// Update empty status
	defer q.Empty.Set(false)

	if !q.useHandover.Load() {
		isHandover = false
	} else {
		isHandover = q.handover.tryOpen(batchLen)
		if !isHandover {
			q.logger.Error("failed to open handover")
		}
	}

	if isHandover {
		for i, item := range items {
			if item == nil {
				q.logger.Error("cannot enqueue nil item")
				continue
			}

			b, err := encodeItem(item)
			if err != nil {
				q.logger.Error("failed to encode item", "err", err)
				continue
			}

			encodedItem := &handoverEncodedItem{
				bytes: b,
				item:  item,
			}
			if !q.handover.tryPut(encodedItem) {
				q.logger.Error("failed to put item in handover")
				failedHandoverItems = append(failedHandoverItems, encodedItem)
			}

			if i == 0 {
				q.HandoverOpen.Set(true)
			}
		}
	} else {
		wg.Add(len(items))
		for _, item := range items {
			if item == nil {
				q.logger.Error("cannot enqueue nil item")
				continue
			}

			go func(i *Item, wg *sync.WaitGroup) {
				defer wg.Done()

				b, err := encodeItem(i)
				if err != nil {
					q.logger.Error("failed to encode item", "err", err)
					return
				}

				itemsChan <- &handoverEncodedItem{
					bytes: b,
					item:  i,
				}
			}(item, wg)
		}
		wg.Wait()
	}

	// This close IS necessary to avoid indefinitely waiting in the next loop
	// It's also necessary to close the channel if handover was used
	close(itemsChan)

	// Wait for handover to finish then close for consumption
	for q.useHandover.Load() {
		done := <-q.handover.signalConsumerDone
		if done {
			q.HandoverOpen.Set(false)
			break
		}
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Seek to end of file to get start position
	startPos, err := q.queueFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %s", err.Error())
	}

	if isHandover {
		itemsDrained, ok := q.handover.tryDrain()
		if ok {
			for _, item := range itemsDrained {
				if item == nil {
					continue
				}
				if _, err := writeItemToFile(q, item, &startPos); err != nil {
					// Should never happen when commit is not enabled
					if err == ErrCommitValueReceived {
						panic(err)
					}
					return err
				}
			}
		}
		for _, item := range failedHandoverItems {
			if _, err := writeItemToFile(q, item, &startPos); err != nil {
				// Should never happen when commit is not enabled
				if err == ErrCommitValueReceived {
					panic(err)
				}
				return err
			}
		}
		if !q.handover.tryClose() {
			return fmt.Errorf("failed to close handover")
		}
	} else {
		for item := range itemsChan {
			if _, err := writeItemToFile(q, item, &startPos); err != nil {
				// Should never happen when commit is not enabled
				if err == ErrCommitValueReceived {
					panic(err)
				}
				return err
			}
		}
	}

	return nil
}

func (q *PersistentGroupedQueue) enqueueNoCommit(item *Item) error {
	if item == nil {
		return errors.New("cannot enqueue nil item")
	}

	if !q.CanEnqueue() {
		return ErrQueueClosed
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Find free position
	startPos, err := q.queueFile.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek to end of file: %w", err)
	}

	// Encode and write item
	itemBytes, err := encodeItem(item)
	if err != nil {
		return fmt.Errorf("failed to marshal item: %w", err)
	}

	_, err = q.queueFile.Write(itemBytes)
	if err != nil {
		return fmt.Errorf("failed to write item: %w", err)
	}

	// Update host index and order
	commit, err := q.index.Add(item.URL.Host, item.ID, uint64(startPos), uint64(len(itemBytes)))
	if err != nil {
		return fmt.Errorf("failed to update index: %w", err)
	}
	if !q.useCommit && commit != 0 {
		// Should never happen when commit is not enabled
		panic(ErrCommitValueReceived)
	}

	// Update stats
	q.updateEnqueueStats(item)

	// Update empty status
	q.Empty.Set(false)

	return nil
}

func (q *PersistentGroupedQueue) batchEnqueueUntilCommitted(items ...*Item) error {
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

	var (
		// Global
		isHandover bool

		// Handover
		failedHandoverItems = []*handoverEncodedItem{}

		// No Handover
		itemsChan = make(chan *handoverEncodedItem, batchLen)
		wg        = &sync.WaitGroup{}
	)

	// Update empty status
	defer q.Empty.Set(false)

	if !q.useHandover.Load() {
		isHandover = false
	} else {
		isHandover = q.handover.tryOpen(batchLen)
		if !isHandover {
			q.logger.Error("failed to open handover")
		}
	}

	if isHandover {
		for i, item := range items {
			if item == nil {
				q.logger.Error("cannot enqueue nil item")
				continue
			}

			b, err := encodeItem(item)
			if err != nil {
				q.logger.Error("failed to encode item", "err", err)
				continue
			}

			encodedItem := &handoverEncodedItem{
				bytes: b,
				item:  item,
			}
			if !q.handover.tryPut(encodedItem) {
				q.logger.Error("failed to put item in handover")
				failedHandoverItems = append(failedHandoverItems, encodedItem)
			}

			if i == 0 {
				q.HandoverOpen.Set(true)
			}
		}
	} else {
		wg.Add(len(items))
		for _, item := range items {
			if item == nil {
				q.logger.Error("cannot enqueue nil item")
				continue
			}

			go func(i *Item, wg *sync.WaitGroup) {
				defer wg.Done()

				b, err := encodeItem(i)
				if err != nil {
					q.logger.Error("failed to encode item", "err", err)
					return
				}

				itemsChan <- &handoverEncodedItem{
					bytes: b,
					item:  i,
				}
			}(item, wg)
		}
		wg.Wait()
	}

	// This close IS necessary to avoid indefinitely waiting in the next loop
	// It's also necessary to close the channel if handover was used
	close(itemsChan)

	// Wait for handover to finish then close for consumption
	for q.useHandover.Load() {
		done := <-q.handover.signalConsumerDone
		if done {
			q.HandoverOpen.Set(false)
			break
		}
	}

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

		if isHandover {
			itemsDrained, ok := q.handover.tryDrain()
			if ok {
				for _, item := range itemsDrained {
					if item == nil {
						continue
					}
					commit, err = writeItemToFile(q, item, &startPos)
					if err != nil {
						return err
					}
					writtenCount++
				}
			}
			for _, item := range failedHandoverItems {
				commit, err = writeItemToFile(q, item, &startPos)
				if err != nil {
					return err
				}
				writtenCount++
			}
			if !q.handover.tryClose() {
				return fmt.Errorf("failed to close handover")
			}
		} else {
			for item := range itemsChan {
				commit, err = writeItemToFile(q, item, &startPos)
				if err != nil {
					return err
				}
				writtenCount++
			}
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

func (q *PersistentGroupedQueue) enqueueUntilCommitted(item *Item) error {
	if item == nil {
		return errors.New("cannot enqueue nil item")
	}

	if !q.CanEnqueue() {
		return ErrQueueClosed
	}

	var commit uint64 = 0

	// <- lock mutex
	err := func() error {
		q.mutex.Lock()
		defer q.mutex.Unlock()
		// Find free position
		startPos, err := q.queueFile.Seek(0, io.SeekEnd)
		if err != nil {
			return fmt.Errorf("failed to seek to end of file: %w", err)
		}

		// Encode and write item
		itemBytes, err := encodeItem(item)
		if err != nil {
			return fmt.Errorf("failed to marshal item: %w", err)
		}

		_, err = q.queueFile.Write(itemBytes)
		if err != nil {
			return fmt.Errorf("failed to write item: %w", err)
		}

		// Update host index and order
		commit, err = q.index.Add(item.URL.Host, item.ID, uint64(startPos), uint64(len(itemBytes)))
		if err != nil {
			return fmt.Errorf("failed to update index: %w", err)
		}

		return nil // success
	}()
	// below is outside of the mutex lock ->
	if err != nil {
		return err
	}

	q.index.AwaitWALCommitted(commit)

	// Update stats
	q.updateEnqueueStats(item)

	// Update empty status
	q.Empty.Set(false)

	return nil
}

func writeItemToFile(q *PersistentGroupedQueue, item *handoverEncodedItem, startPos *int64) (commit uint64, err error) {
	// Write item to file
	written, err := q.queueFile.Write(item.bytes)
	if err != nil {
		return 0, fmt.Errorf("failed to write item: %w", err)
	}

	// Update host index and order
	commit, err = q.index.Add(item.item.URL.Host, item.item.ID, uint64(*startPos), uint64(len(item.bytes)))
	if err != nil {
		return 0, fmt.Errorf("failed to update index: %w", err)
	}

	*startPos += int64(written)

	// Update stats
	q.updateEnqueueStats(item.item)

	// If commit is not enabled we discard the commit value which should already be zero
	if !q.useCommit && commit != 0 {
		return 0, ErrCommitValueReceived
	}
	return commit, nil
}
