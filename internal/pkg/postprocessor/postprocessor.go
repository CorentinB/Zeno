package postprocessor

import (
	"context"
	"sync"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
)

type postprocessor struct {
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	inputCh  chan *models.Item
	outputCh chan *models.Item
	errorCh  chan *models.Item
}

var (
	globalPostprocessor *postprocessor
	once                sync.Once
	logger              *log.FieldedLogger
)

// This functions starts the preprocessor responsible for preparing
// the seeds sent by the reactor for captures
func Start(inputChan, outputChan, errorChan chan *models.Item) error {
	var done bool

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "postprocessor",
	})

	stats.Init()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		globalPostprocessor = &postprocessor{
			ctx:      ctx,
			cancel:   cancel,
			inputCh:  inputChan,
			outputCh: outputChan,
			errorCh:  errorChan,
		}
		logger.Debug("initialized")
		globalPostprocessor.wg.Add(1)
		go run()
		logger.Info("started")
		done = true
	})

	if !done {
		return ErrPostprocessorAlreadyInitialized
	}

	return nil
}

func Stop() {
	if globalPostprocessor != nil {
		globalPostprocessor.cancel()
		globalPostprocessor.wg.Wait()
		close(globalPostprocessor.outputCh)
		logger.Info("stopped")
	}
}

func run() {
	defer globalPostprocessor.wg.Done()

	var (
		wg    sync.WaitGroup
		guard = make(chan struct{}, config.Get().WorkersCount)
	)

	for {
		select {
		// Closes the run routine when context is canceled
		case <-globalPostprocessor.ctx.Done():
			logger.Debug("shutting down")
			return
		case item, ok := <-globalPostprocessor.inputCh:
			if ok {
				logger.Debug("received item", "item", item.GetShortID())
				guard <- struct{}{}
				wg.Add(1)
				stats.PostprocessorRoutinesIncr()
				go func() {
					defer wg.Done()
					defer func() { <-guard }()
					defer stats.PostprocessorRoutinesDecr()
					postprocess(item)
					globalPostprocessor.outputCh <- item
				}()
			}
		}
	}
}

func postprocess(item *models.Item) {
	if item.GetStatus() != models.ItemFailed {
		item.SetRedirection(nil)
		return
	}

	defer item.SetStatus(models.ItemPostProcessed)

	// TODO: execute assets redirection
	var URL *models.URL

	if item.GetRedirection() != nil {
		URL = item.GetRedirection()
	} else {
		URL = item.GetURL()
	}

	// Verify if there is any redirection
	if isStatusCodeRedirect(URL.GetResponse().StatusCode) {
		// Check if the current redirections count doesn't exceed the max allowed
		if URL.GetRedirects() >= config.Get().MaxRedirect {
			logger.Warn("max redirects reached", "item", item.GetShortID())
			return
		}

		// Prepare the new item resulting from the redirection
		item.SetRedirection(&models.URL{
			Raw:       URL.GetResponse().Header.Get("Location"),
			Redirects: URL.GetRedirects() + 1,
			Hops:      URL.GetHops(),
		})

		return
	} else {
		item.SetRedirection(nil)
	}
}
