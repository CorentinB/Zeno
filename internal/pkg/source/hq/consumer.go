package hq

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/davecgh/go-spew/spew"
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

			// Cancel the context to stop all goroutines.
			cancel()

			logger.Debug("waiting for goroutines to finish")

			// Wait for all goroutines to finish
			wg.Wait()

			// Close the urlBuffer to signal consumerSenders to finish
			close(urlBuffer)

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

	// var previousBatchReceived []gocrawlhq.URL

	for {
		// Check for context cancellation
		select {
		case <-ctx.Done():
			logger.Debug("closed")
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

		err = ensureAllURLsUnique(URLs)
		if err != nil {
			spew.Dump(URLs)
			panic(err)
		}

		// // Debug check to troubleshoot a problem where the same seed might be received twice in contiguous batches
		// if previousBatchReceived != nil {
		// 	// Concat the previous and current batch
		// 	concatedBatches := append(previousBatchReceived, URLs...)
		// 	err := ensureAllURLsUnique(concatedBatches)
		// 	if err != nil {
		// 		spew.Dump(concatedBatches)
		// 		panic(err)
		// 	}
		// }
		// // Create a deep copy of URLs to previousBatchReceived
		// previousBatchReceived = make([]gocrawlhq.URL, len(URLs))
		// copy(previousBatchReceived, URLs)

		// Enqueue URLs into the buffer
		for i := range URLs {
			select {
			case <-ctx.Done():
				logger.Debug("closed")
				return
			case urlBuffer <- &gocrawlhq.URL{
				ID:        URLs[i].ID,
				Value:     URLs[i].Value,
				Via:       URLs[i].Via,
				Host:      URLs[i].Host,
				Path:      URLs[i].Path,
				Type:      URLs[i].Type,
				Crawler:   URLs[i].Crawler,
				Status:    URLs[i].Status,
				LiftOff:   URLs[i].LiftOff,
				Timestamp: URLs[i].Timestamp,
			}: //Deep copy of the URL to ensure pointer alisaing does not cause issues
			}
		}

		// Empty the URL slice
		URLs = nil
	}
}

func consumerSender(ctx context.Context, wg *sync.WaitGroup, urlBuffer <-chan *gocrawlhq.URL) {
	defer wg.Done()

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "hq.consumerSender",
	})

	// var previousURLReceived *gocrawlhq.URL

	for {
		select {
		case <-ctx.Done():
			logger.Debug("closed")
			return
		case URL := <-urlBuffer:
			// // Debug check to troubleshoot a problem where the same seed is received twice by the reactor
			// if previousURLReceived != nil && previousURLReceived.ID == URL.ID {
			// 	spew.Dump(previousURLReceived)
			// 	spew.Dump(URL)
			// 	panic("same seed received twice by hq.consumerSender")
			// }
			// urlCopy := *URL
			// previousURLReceived = &urlCopy

			var discard bool
			// Process the URL and create a new Item
			parsedURL := models.URL{
				Raw:  URL.Value,
				Hops: pathToHops(URL.Path),
			}
			err := parsedURL.Parse()
			if err != nil {
				discard = true
			}
			newItem := models.NewItem(URL.ID, &parsedURL, URL.Via, true)
			newItem.SetStatus(models.ItemFresh)
			newItem.SetSource(models.ItemSourceHQ)

			if discard {
				logger.Debug("parsing failed, sending the item to finisher", "url", URL.Value)
				globalHQ.finishCh <- newItem
				break
			}

			logger.Debug("sending new item to reactor", "item", newItem.GetShortID())

			// Send the new Item to the reactor
			err = reactor.ReceiveInsert(newItem)
			if err != nil {
				if err == reactor.ErrReactorFrozen {
					select {
					case <-ctx.Done():
						logger.Debug("closed while sending to frozen reactor")
						return
					}
				}
				if err != reactor.ErrItemAlreadyPresent {
					panic(err)
				}
			}
		}
	}
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
		if len(URLs) != 0 {
			allURLs = append(allURLs, URLs...)
		}
	}

	return allURLs, nil
}

func ensureAllURLsUnique(URLs []gocrawlhq.URL) error {
	seen := make(map[string]struct{})
	for _, URL := range URLs {
		if _, ok := seen[URL.ID]; ok {
			return errors.New("duplicate URL ID found")
		}
		seen[URL.ID] = struct{}{}
	}
	return nil
}
