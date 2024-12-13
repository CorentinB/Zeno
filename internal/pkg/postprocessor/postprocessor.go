package postprocessor

import (
	"context"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/controler/pause"
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
}

var (
	globalPostprocessor *postprocessor
	once                sync.Once
	logger              *log.FieldedLogger
)

// This functions starts the preprocessor responsible for preparing
// the seeds sent by the reactor for captures
func Start(inputChan, outputChan chan *models.Item) error {
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
		logger.Info("stopped")
	}
}

func run() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "postprocessor.run",
	})

	defer globalPostprocessor.wg.Done()

	// Create a context to manage goroutines
	ctx, cancel := context.WithCancel(globalPostprocessor.ctx)
	defer cancel()

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Guard to limit the number of concurrent archiver routines
	guard := make(chan struct{}, config.Get().WorkersCount)

	// Subscribe to the pause controler
	controlChans := pause.Subscribe()
	defer pause.Unsubscribe(controlChans)

	for {
		select {
		case <-controlChans.PauseCh:
			logger.Debug("received pause event")
			controlChans.ResumeCh <- struct{}{}
			logger.Debug("received resume event")
		// Closes the run routine when context is canceled
		case <-globalPostprocessor.ctx.Done():
			logger.Debug("shutting down")
			wg.Wait()
			return
		case item, ok := <-globalPostprocessor.inputCh:
			if ok {
				logger.Debug("received item", "item", item.GetShortID())
				guard <- struct{}{}
				wg.Add(1)
				stats.PostprocessorRoutinesIncr()
				go func(ctx context.Context) {
					defer wg.Done()
					defer func() { <-guard }()
					defer stats.PostprocessorRoutinesDecr()

					if item.GetStatus() != models.ItemFailed && item.GetStatus() != models.ItemCompleted {
						postprocess(item)
					} else {
						logger.Debug("skipping item", "item", item.GetShortID(), "status", item.GetStatus().String())
					}

					select {
					case <-ctx.Done():
						logger.Debug("aborting item due to stop", "item", item.GetShortID())
						return
					case globalPostprocessor.outputCh <- item:
					}
				}(ctx)
			}
		}
	}
}

func postprocess(item *models.Item) {
	// If we don't capture assets, there is no need to postprocess the item
	// TODO: handle hops even with disable assets capture
	if config.Get().DisableAssetsCapture {
		return
	}

	items, err := item.GetNodesAtLevel(item.GetMaxDepth())
	if err != nil {
		logger.Error("unable to get nodes at level", "err", err.Error(), "item", item.GetShortID())
		panic(err)
	}

	for i := range items {

		if items[i].GetStatus() != models.ItemArchived {
			logger.Debug("item not archived, skipping", "item", items[i].GetShortID())
			return
		}

		// Verify if there is any redirection
		// TODO: execute assets redirection
		if isStatusCodeRedirect(items[i].GetURL().GetResponse().StatusCode) {
			// Check if the current redirections count doesn't exceed the max allowed
			if items[i].GetURL().GetRedirects() >= config.Get().MaxRedirect {
				logger.Warn("max redirects reached", "item", item.GetShortID(), "func", "postprocessor.postprocess")
				return
			}

			// Prepare the new item resulting from the redirection
			newURL := &models.URL{
				Raw:       items[i].GetURL().GetResponse().Header.Get("Location"),
				Redirects: items[i].GetURL().GetRedirects() + 1,
				Hops:      items[i].GetURL().GetHops(),
			}

			items[i].SetStatus(models.ItemGotRedirected)
			err := items[i].AddChild(models.NewItem(uuid.New().String(), newURL, "", false), items[i].GetStatus())
			if err != nil {
				panic(err)
			}

			return
		}

		// Return if:
		// - the item is a child and the URL has more than one hop
		// - assets capture is disabled and domains crawl is disabled
		// - the URL has more hops than the max allowed
		if (items[i].IsChild() && items[i].GetURL().GetHops() > 1) ||
			config.Get().DisableAssetsCapture && !config.Get().DomainsCrawl && (uint64(config.Get().MaxHops) <= uint64(items[i].GetURL().GetHops())) {
			return
		}

		if items[i].GetURL().GetResponse() != nil {
			// Generate the goquery document from the response body
			doc, err := goquery.NewDocumentFromReader(items[i].GetURL().GetBody())
			if err != nil {
				logger.Error("unable to create goquery document", "err", err.Error(), "item", items[i].GetShortID())
				return
			}

			items[i].GetURL().RewindBody()

			// If the URL is a seed, scrape the base tag
			if items[i].IsSeed() || items[i].IsRedirection() {
				scrapeBaseTag(doc, items[i])
			}

			// Extract assets from the document
			assets, err := extractAssets(doc, items[i].GetURL(), items[i])
			if err != nil {
				logger.Error("unable to extract assets", "err", err.Error(), "item", items[i].GetShortID())
			}

			for _, asset := range assets {
				if assets == nil {
					logger.Warn("nil asset", "item", items[i].GetShortID())
					continue
				}

				items[i].SetStatus(models.ItemGotChildren)
				items[i].AddChild(models.NewItem(uuid.New().String(), asset, "", false), items[i].GetStatus())
			}
		}

		if items[i].GetStatus() != models.ItemGotChildren && items[i].GetStatus() != models.ItemGotRedirected {
			items[i].SetStatus(models.ItemCompleted)
		}
	}
}
