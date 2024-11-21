package hq

import (
	"context"
	"sync"
	"time"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/reactor"
	"github.com/internetarchive/Zeno/pkg/models"
	"github.com/internetarchive/gocrawlhq"
)

func consumer() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.consumer",
	})

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalHQ.ctx)
	defer cancel()

	// Set the batch size for fetching URLs
	batchSize := config.Get().HQBatchSize

	// Create a fixed-size buffer (channel) for URLs
	urlBuffer := make(chan *gocrawlhq.URL, batchSize)

	// WaitGroup to wait for goroutines to finish on shutdown
	var wg sync.WaitGroup

	// Start the consumerFetcher goroutine(s)
	wg.Add(1)
	go consumerFetcher(ctx, &wg, urlBuffer, batchSize)

	// Start the consumerSender goroutine(s)
	wg.Add(1)
	go consumerSender(ctx, &wg, urlBuffer)

	// Wait for shutdown signal
	for {
		select {
		case <-globalHQ.ctx.Done():
			logger.Debug("received done signal")
			logger.Debug("waiting for goroutines to finish")
			// Close the urlBuffer to signal consumerSenders to finish
			close(urlBuffer)

			// Wait for all goroutines to finish
			wg.Wait()

			globalHQ.wg.Done()

			logger.Debug("closed")
			return
		}
	}
}

func consumerFetcher(ctx context.Context, wg *sync.WaitGroup, urlBuffer chan<- *gocrawlhq.URL, batchSize int) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.consumerFetcher",
	})

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			return
		default:
		}

		// Fetch URLs from HQ
		URLs, err := getURLs(batchSize)
		if err != nil {
			if err.Error() == "gocrawlhq: feed is empty" {
				logger.Debug("feed is empty, waiting for new URLs")
			} else {
				logger.Error("error fetching URLs from CrawlHQ", "err", err.Error(), "func", "hq.consumerFetcher")
			}
			time.Sleep(250 * time.Millisecond)
			continue
		}

		// Enqueue URLs into the buffer
		for _, URL := range URLs {
			select {
			case <-ctx.Done():
				logger.Debug("closing")
				return
			case urlBuffer <- &URL:
			}
		}
	}
}

func consumerSender(ctx context.Context, wg *sync.WaitGroup, urlBuffer <-chan *gocrawlhq.URL) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.consumerSender",
	})

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closing")
			return
		case URL, ok := <-urlBuffer:
			if !ok {
				logger.Debug("closing")
				return
			}

			// Process the URL and send to reactor
			err := processAndSend(URL)
			if err != nil && err != reactor.ErrReactorFrozen {
				panic(err)
			}
		}
	}
}

func processAndSend(URL *gocrawlhq.URL) error {
	newItem := &models.Item{
		ID: URL.ID,
		URL: &models.URL{
			Raw:  URL.Value,
			Hops: pathToHops(URL.Path),
		},
		Via:    URL.Via,
		Status: models.ItemFresh,
		Source: models.ItemSourceHQ,
	}

	// Send the item to the reactor
	err := reactor.ReceiveInsert(newItem)
	if err != nil {
		return err
	}
	return nil
}

func getURLs(batchSize int) ([]gocrawlhq.URL, error) {
	// Fetch URLs from CrawlHQ with optional concurrency
	if config.Get().HQBatchConcurrency == 1 {
		return globalHQ.client.Get(batchSize, config.Get().HQStrategy)
	}

	var wg sync.WaitGroup
	concurrency := config.Get().HQBatchConcurrency
	subBatchSize := batchSize / concurrency
	urlsChan := make(chan []gocrawlhq.URL, concurrency)
	var allURLs []gocrawlhq.URL

	// Start concurrent fetches
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			URLs, err := globalHQ.client.Get(subBatchSize, config.Get().HQStrategy)
			if err != nil {
				logger.Error("error fetching URLs from CrawlHQ", "err", err.Error(), "func", "hq.getURLs")
				return
			}
			urlsChan <- URLs
		}()
	}

	// Wait for all fetches to complete
	wg.Wait()
	close(urlsChan)

	// Collect URLs from all fetches
	for URLs := range urlsChan {
		allURLs = append(allURLs, URLs...)
	}

	return allURLs, nil
}
