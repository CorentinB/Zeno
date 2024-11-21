package hq

import (
	"context"
	"sync"
	"time"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/gocrawlhq"
)

// producerBatch represents a batch of URLs to be added to HQ.
type producerBatch struct {
	URLs []gocrawlhq.URL
}

// producer initializes and starts the producer and dispatcher processes.
func producer() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.producer",
	})

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalHQ.ctx)
	defer cancel()

	maxSenders := getMaxProducerSenders()
	batchCh := make(chan *producerBatch, maxSenders)

	var wg sync.WaitGroup

	wg.Add(1)
	go producerReceiver(ctx, &wg, batchCh)

	wg.Add(1)
	go producerDispatcher(ctx, &wg, batchCh)

	// Wait for the context to be canceled.
	for {
		select {
		case <-globalHQ.ctx.Done():
			logger.Debug("received done signal")
			// Cancel the context to stop all goroutines.
			cancel()

			logger.Debug("waiting for goroutines to finish")
			// Wait for the producer and dispatcher to finish.
			wg.Wait()

			// Close the batch channel to signal the dispatcher to finish.
			close(batchCh)

			globalHQ.wg.Done()

			logger.Debug("closed")
			return
		}
	}
}

// producerReceiver reads URLs from produceCh, accumulates them into batches, and sends the batches to batchCh.
func producerReceiver(ctx context.Context, wg *sync.WaitGroup, batchCh chan *producerBatch) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.producerReceiver",
	})

	batchSize := getProducerBatchSize()
	maxWaitTime := 5 * time.Second

	batch := &producerBatch{
		URLs: make([]gocrawlhq.URL, 0, batchSize),
	}
	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			// Send any remaining URLs.
			if len(batch.URLs) > 0 {
				logger.Debug("while closing, sending remaining batch to dispatcher", "size", len(batch.URLs))
				batchCh <- batch // Blocks if batchCh is full.
			}
			return
		case item := <-globalHQ.produceCh:
			URL := gocrawlhq.URL{
				Value: item.URL.String(),
				Via:   item.Via,
				Path:  hopsToPath(item.URL.GetHops()),
			}
			batch.URLs = append(batch.URLs, URL)
			if len(batch.URLs) >= batchSize {
				logger.Debug("sending batch to dispatcher", "size", len(batch.URLs))
				// Send the batch to batchCh.
				copyBatch := *batch
				batchCh <- &copyBatch // Blocks if batchCh is full.
				batch = &producerBatch{
					URLs: make([]gocrawlhq.URL, 0, batchSize),
				}
				resetTimer(timer, maxWaitTime)
			}
		case <-timer.C:
			if len(batch.URLs) > 0 {
				logger.Debug("sending non-full batch to dispatcher", "size", len(batch.URLs))
				copyBatch := *batch
				batchCh <- &copyBatch // Blocks if batchCh is full.
				batch = &producerBatch{
					URLs: make([]gocrawlhq.URL, 0, batchSize),
				}
			}
			resetTimer(timer, maxWaitTime)
		}
	}
}

// producerDispatcher receives batches from batchCh and dispatches them to sender routines.
func producerDispatcher(ctx context.Context, wg *sync.WaitGroup, batchCh chan *producerBatch) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.producerDispatcher",
	})

	maxSenders := getMaxProducerSenders()
	senderSemaphore := make(chan struct{}, maxSenders)
	var producerWg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			// Wait for all sender routines to finish.
			producerWg.Wait()
			return
		case batch, ok := <-batchCh:
			if !ok {
				logger.Debug("closing")
				// Wait for all sender routines to finish.
				producerWg.Wait()
				return
			}

			senderSemaphore <- struct{}{} // Blocks if maxSenders reached.
			producerWg.Add(1)
			logger.Debug("dispatching batch to sender", "size", len(batch.URLs))
			go func(batch *producerBatch) {
				defer producerWg.Done()
				defer func() { <-senderSemaphore }()
				producerSender(ctx, batch)
			}(batch)
		}
	}
}

// producerSender sends a batch of URLs to HQ with retries and exponential backoff.
func producerSender(ctx context.Context, batch *producerBatch) {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.producerSender",
	})

	backoff := time.Second
	maxBackoff := 5 * time.Second

	logger.Debug("sending batch to HQ", "size", len(batch.URLs))

	for {
		err := globalHQ.client.Add(batch.URLs, false) // Use bypassSeencheck = false
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			return
		default:
			if err != nil {
				logger.Error("error sending batch to HQ", "err", err)
				time.Sleep(backoff)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
				continue
			}
			return
		}
	}
}

// getMaxProducerSenders returns the maximum number of sender routines based on configuration.
func getMaxProducerSenders() int {
	workersCount := config.Get().WorkersCount
	if workersCount < 10 {
		return 1
	}
	return workersCount / 10
}

// getProducerBatchSize returns the batch size based on configuration.
func getProducerBatchSize() int {
	batchSize := config.Get().HQBatchSize
	if batchSize == 0 {
		batchSize = 100 // Default batch size.
	}
	return batchSize
}
