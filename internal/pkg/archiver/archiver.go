package archiver

import (
	"context"
	"io"
	"net/http"
	"sync"

	"github.com/CorentinB/warc"
	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
)

type archiver struct {
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	inputCh  chan *models.Item
	outputCh chan *models.Item

	Client          *warc.CustomHTTPClient
	ClientWithProxy *warc.CustomHTTPClient
}

var (
	globalArchiver *archiver
	once           sync.Once
	logger         *log.FieldedLogger
)

// This functions starts the archiver responsible for capturing the URLs
func Start(inputChan, outputChan chan *models.Item) error {
	var done bool

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "archiver",
	})

	stats.Init()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		globalArchiver = &archiver{
			ctx:      ctx,
			cancel:   cancel,
			inputCh:  inputChan,
			outputCh: outputChan,
		}
		logger.Debug("initialized")

		// Setup WARC writing HTTP clients
		startWARCWriter()

		globalArchiver.wg.Add(1)
		go run()
		logger.Info("started")
		done = true
	})

	if !done {
		return ErrArchiverAlreadyInitialized
	}

	return nil
}

func Stop() {
	if globalArchiver != nil {
		globalArchiver.cancel()
		globalArchiver.wg.Wait()

		// Wait for the WARC writing to finish
		globalArchiver.Client.WaitGroup.Wait()
		globalArchiver.Client.Close()
		if globalArchiver.ClientWithProxy != nil {
			globalArchiver.ClientWithProxy.WaitGroup.Wait()
			globalArchiver.ClientWithProxy.Close()
		}

		logger.Info("stopped")
	}
}

func run() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "archiver.run",
	})

	defer globalArchiver.wg.Done()

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalArchiver.ctx)
	defer cancel()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Guard to limit the number of concurrent archiver routines
	guard := make(chan struct{}, config.Get().WorkersCount)

	for {
		select {
		// Closes the run routine when context is canceled
		case <-globalArchiver.ctx.Done():
			logger.Debug("shutting down")
			wg.Wait()
			return
		case item, ok := <-globalArchiver.inputCh:
			if ok {
				logger.Debug("received item", "item", item.GetShortID())
				guard <- struct{}{}
				wg.Add(1)
				stats.ArchiverRoutinesIncr()
				go func(ctx context.Context) {
					defer wg.Done()
					defer func() { <-guard }()
					defer stats.ArchiverRoutinesDecr()

					archive(item)

					select {
					case <-ctx.Done():
						return
					case globalArchiver.outputCh <- item:
					}
				}(ctx)
			}
		}
	}
}

func archive(item *models.Item) {
	// TODO: rate limiting handling

	var (
		URLsToCapture []*models.URL
		guard         = make(chan struct{}, config.Get().MaxConcurrentAssets)
		wg            sync.WaitGroup
		itemState     = models.ItemCaptured
	)

	// Determines the URLs that need to be captured, if the item's status is fresh we need
	// to capture the seed, else if it's a redirection we need to captue it, and
	// else we need to capture the child URLs (assets), in parallel
	if item.GetRedirection() != nil {
		URLsToCapture = append(URLsToCapture, item.GetRedirection())
	} else if item.GetStatus() == models.ItemPreProcessed {
		URLsToCapture = append(URLsToCapture, item.GetURL())
	} else {
		URLsToCapture = item.GetChilds()
	}

	for _, URL := range URLsToCapture {
		guard <- struct{}{}
		wg.Add(1)
		go func(URL *models.URL) {
			defer wg.Done()
			defer func() { <-guard }()

			var (
				err  error
				resp *http.Response
			)

			if config.Get().Proxy != "" {
				resp, err = globalArchiver.ClientWithProxy.Do(URL.GetRequest())
			} else {
				resp, err = globalArchiver.Client.Do(URL.GetRequest())
			}
			if err != nil {
				logger.Error("unable to execute request", "err", err.Error(), "func", "archiver.archive")

				// Only mark the item as failed if we were processing a redirection or a new seed
				if item.GetStatus() == models.ItemFresh || item.GetRedirection() != nil {
					itemState = models.ItemFailed
				}

				return
			}

			stats.URLsCrawledIncr()

			// Set the response in the item
			URL.SetResponse(resp)

			// For now, we only consume it
			_, err = io.Copy(io.Discard, resp.Body)
			if err != nil {
				logger.Error("unable to consume response body", "url", URL.String(), "err", err.Error(), "func", "archiver.archive")
			}
		}(URL)
	}

	wg.Wait()

	item.SetStatus(itemState)
}
