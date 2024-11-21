package hq

import (
	"context"
	"sync"
	"time"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/gocrawlhq"
)

type finishBatch struct {
	URLs           []gocrawlhq.URL
	ChildsCaptured int
}

// finisher initializes and starts the finisher and dispatcher processes.
func finisher() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.finisher",
	})

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalHQ.ctx)
	defer cancel()

	maxSenders := getMaxFinishSenders()
	batchCh := make(chan *finishBatch, maxSenders)

	var wg sync.WaitGroup

	wg.Add(1)
	go finisherReceiver(ctx, &wg, batchCh)

	wg.Add(1)
	go finisherDispatcher(ctx, &wg, batchCh)

	// Wait for the context to be canceled.
	for {
		select {
		case <-globalHQ.ctx.Done():
			logger.Debug("received done signal")
			logger.Debug("waiting for goroutines to finish")

			// Close the batch channel to signal the dispatcher to finish.
			close(batchCh)

			// Wait for the finisher and dispatcher to finish.
			wg.Wait()

			globalHQ.wg.Done()

			logger.Debug("closed")
			return
		}
	}
}

// finisherReceiver reads URLs from finishCh, accumulates them into batches, and sends the batches to batchCh.
func finisherReceiver(ctx context.Context, wg *sync.WaitGroup, batchCh chan *finishBatch) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.finisherReceiver",
	})

	batchSize := getBatchSize()
	maxWaitTime := 5 * time.Second

	batch := &finishBatch{
		URLs: make([]gocrawlhq.URL, 0, batchSize),
	}
	timer := time.NewTimer(maxWaitTime)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			return
		case item := <-globalHQ.finishCh:
			logger.Debug("received item", "item", item.GetShortID())
			URL := gocrawlhq.URL{
				ID:   item.ID,
				Type: "seed",
			}
			batch.URLs = append(batch.URLs, URL)
			if len(batch.URLs) >= batchSize {
				logger.Debug("sending batch to dispatcher", "size", len(batch.URLs))
				// Send the batch to batchCh.
				copyBatch := *batch
				batchCh <- &copyBatch // Blocks if batchCh is full.
				batch = &finishBatch{
					URLs: make([]gocrawlhq.URL, 0, batchSize),
				}
				resetTimer(timer, maxWaitTime)
			}
		case <-timer.C:
			if len(batch.URLs) > 0 {
				logger.Debug("sending non-full batch to dispatcher", "size", len(batch.URLs))
				copyBatch := *batch
				batchCh <- &copyBatch // Blocks if batchCh is full.
				batch = &finishBatch{
					URLs: make([]gocrawlhq.URL, 0, batchSize),
				}
			}
			resetTimer(timer, maxWaitTime)
		}
	}
}

// finisherDispatcher receives batches from batchCh and dispatches them to sender routines.
func finisherDispatcher(ctx context.Context, wg *sync.WaitGroup, batchCh chan *finishBatch) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.finisherDispatcher",
	})

	maxSenders := getMaxFinishSenders()
	senderSemaphore := make(chan struct{}, maxSenders)
	var senderWg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			// Wait for all sender routines to finish.
			senderWg.Wait()
			return
		case batch, ok := <-batchCh:
			if !ok {
				logger.Debug("closing")
				// Wait for all sender routines to finish.
				senderWg.Wait()
				return
			}

			senderSemaphore <- struct{}{} // Blocks if maxSenders reached.
			senderWg.Add(1)
			logger.Debug("dispatching batch to sender", "size", len(batch.URLs))
			go func(batch *finishBatch) {
				defer senderWg.Done()
				defer func() { <-senderSemaphore }()
				finisherSender(ctx, batch)
			}(batch)
		}
	}
}

// finisherSender sends a batch of URLs to HQ with retries and exponential backoff.
func finisherSender(ctx context.Context, batch *finishBatch) {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.finisherSender",
	})

	backoff := time.Second
	maxBackoff := 5 * time.Second

	logger.Debug("sending batch to HQ", "size", len(batch.URLs))

	for {
		err := globalHQ.client.Delete(batch.URLs, batch.ChildsCaptured)
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

// getMaxFinishSenders returns the maximum number of sender routines based on configuration.
func getMaxFinishSenders() int {
	workersCount := config.Get().WorkersCount
	if workersCount < 10 {
		return 1
	}
	return workersCount / 10
}

// getBatchSize returns the batch size based on configuration.
func getBatchSize() int {
	batchSize := config.Get().WorkersCount
	if batchSize == 0 {
		batchSize = 100 // Default batch size.
	}
	return batchSize
}
