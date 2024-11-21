package preprocessor

import (
	"context"
	"net/http"
	"sync"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/preprocessor/seencheck"
	"github.com/internetarchive/Zeno/internal/pkg/source/hq"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
)

type preprocessor struct {
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	inputCh  chan *models.Item
	outputCh chan *models.Item
}

var (
	globalPreprocessor *preprocessor
	once               sync.Once
	logger             *log.FieldedLogger
)

// This functions starts the preprocessor responsible for preparing
// the seeds sent by the reactor for captures
func Start(inputChan, outputChan chan *models.Item) error {
	var done bool

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "preprocessor",
	})

	stats.Init()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		globalPreprocessor = &preprocessor{
			ctx:      ctx,
			cancel:   cancel,
			inputCh:  inputChan,
			outputCh: outputChan,
		}
		logger.Debug("initialized")
		globalPreprocessor.wg.Add(1)
		go run()
		logger.Info("started")
		done = true
	})

	if !done {
		return ErrPreprocessorAlreadyInitialized
	}

	return nil
}

func Stop() {
	if globalPreprocessor != nil {
		globalPreprocessor.cancel()
		globalPreprocessor.wg.Wait()
		logger.Info("stopped")
	}
}

func run() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "preprocessor.run",
	})

	defer globalPreprocessor.wg.Done()

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalPreprocessor.ctx)
	defer cancel()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Guard to limit the number of concurrent archiver routines
	guard := make(chan struct{}, config.Get().WorkersCount)

	for {
		select {
		// Closes the run routine when context is canceled
		case <-globalPreprocessor.ctx.Done():
			logger.Debug("shutting down")
			return
		case item, ok := <-globalPreprocessor.inputCh:
			if ok {
				logger.Debug("received item", "item", item.GetShortID())
				guard <- struct{}{}
				wg.Add(1)
				stats.PreprocessorRoutinesIncr()
				go func(ctx context.Context) {
					defer wg.Done()
					defer func() { <-guard }()
					defer stats.PreprocessorRoutinesDecr()

					preprocess(item)

					select {
					case <-ctx.Done():
						return
					case globalPreprocessor.outputCh <- item:
					}
				}(ctx)
			}
		}
	}
}

func preprocess(item *models.Item) {
	defer item.SetStatus(models.ItemPreProcessed)

	// Validate the URL of either the item itself and/or its childs
	// TODO: if an error happen and it's a fresh item, we should mark it as failed in HQ (if it's a HQ-based crawl)

	var (
		URLsToPreprocess []*models.URL
		URLType          models.URLType
		err              error
	)

	if item.GetStatus() == models.ItemFresh {
		URLType = models.URLTypeSeed
		URLsToPreprocess = append(URLsToPreprocess, item.GetURL())
	} else if item.GetRedirection() != nil {
		URLType = models.URLTypeRedirection
		URLsToPreprocess = append(URLsToPreprocess, item.GetRedirection())
	} else if len(item.Childs) > 0 {
		URLType = models.URLTypeAsset
		URLsToPreprocess = append(URLsToPreprocess, item.GetChilds()...)
	} else {
		panic("item has no URL to preprocess")
	}

	// Validate the URLs
	for i := 0; i < len(URLsToPreprocess); {
		var parentURL *models.URL

		if URLType != models.URLTypeSeed {
			parentURL = item.GetURL()
		}

		err = normalizeURL(URLsToPreprocess[i], parentURL)
		if err != nil {
			// If we can't validate an URL, we remove it from the list of childs
			logger.Warn("unable to validate URL", "url", URLsToPreprocess[i].Raw, "err", err.Error(), "func", "preprocessor.preprocess")
			URLsToPreprocess = append(URLsToPreprocess[:i], URLsToPreprocess[i+1:]...)
		} else {
			i++
		}
	}

	// If the item is a redirection or an asset, we need to seencheck it if needed
	if config.Get().UseSeencheck && URLType != models.URLTypeSeed {
		var seencheckedURLs []*models.URL

		if config.Get().HQ {
			seencheckedURLs, err = hq.SeencheckURLs(string(URLType), item.URL)
			if err != nil {
				logger.Warn("unable to seencheck URL", "url", item.URL.Raw, "err", err.Error(), "func", "preprocessor.preprocess")
				return
			}
		} else {
			seencheckedURLs, err = seencheck.SeencheckURLs(string(URLType), item.URL)
			if err != nil {
				logger.Warn("unable to seencheck URL", "url", item.URL.Raw, "err", err.Error(), "func", "preprocessor.preprocess")
				return
			}
		}

		switch URLType {
		case models.URLTypeRedirection:
			item.SetRedirection(nil)
		case models.URLTypeAsset:
			item.SetChilds(seencheckedURLs)
		}
	}

	// Finally, we build the requests, applying any site-specific behavior needed
	for _, URL := range URLsToPreprocess {
		// TODO: apply site-specific stuff
		req, err := http.NewRequest(http.MethodGet, URL.String(), nil)
		if err != nil {
			logger.Error("unable to create new request for URL", "url", item.URL.String(), "err", err.Error(), "func", "preprocessor.preprocess")
			return
		}

		URL.SetRequest(req)
	}
}
