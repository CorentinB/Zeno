package finisher

import (
	"context"
	"sync"

	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/reactor"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
)

type finisher struct {
	ctx              context.Context
	cancel           context.CancelFunc
	inputCh          chan *models.Item
	sourceFinishedCh chan *models.Item
	sourceProducedCh chan *models.Item
	wg               sync.WaitGroup
}

var (
	globalFinisher *finisher
	once           sync.Once
	logger         *log.FieldedLogger
)

// Start initializes the global finisher with the given input channel.
// This method can only be called once.
func Start(inputChan, sourceFinishedChan, sourceProducedChan chan *models.Item) error {
	var done bool

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "finisher",
	})

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		globalFinisher = &finisher{
			ctx:              ctx,
			cancel:           cancel,
			inputCh:          inputChan,
			sourceFinishedCh: sourceFinishedChan,
			sourceProducedCh: sourceProducedChan,
			wg:               sync.WaitGroup{},
		}
		logger.Debug("initialized")
		globalFinisher.wg.Add(1)
		go globalFinisher.run()
		logger.Info("started")
		done = true
	})

	if !done {
		return ErrFinisherAlreadyInitialized
	}

	return nil
}

// Stop stops the global finisher.
func Stop() {
	if globalFinisher != nil {
		logger.Debug("received stop signal")
		globalFinisher.cancel()
		globalFinisher.wg.Wait()
		globalFinisher = nil
		once = sync.Once{}
		logger.Info("stopped")
	}
}

func (f *finisher) run() {
	defer f.wg.Done()

	for {
		select {
		case <-f.ctx.Done():
			logger.Debug("shutting down")
			return
		case item := <-f.inputCh:
			if item == nil {
				panic("received nil item")
			}

			logger.Debug("received item", "item", item.GetShortID())

			if item.GetStatus() == models.ItemFresh {
				logger.Debug("fresh item received", "item", item)
				f.sourceProducedCh <- item
			} else if item.GetRedirection() != nil {
				logger.Debug("item has redirection", "item", item.GetShortID())
				err := reactor.ReceiveFeedback(item)
				if err != nil {
					panic(err)
				}
			} else if len(item.GetChilds()) != 0 {
				logger.Debug("item has children", "item", item.GetShortID())
				err := reactor.ReceiveFeedback(item)
				if err != nil {
					panic(err)
				}
			} else {
				logger.Debug("item has no redirection or children", "item", item.GetShortID())
				err := reactor.MarkAsFinished(item)
				if err != nil {
					panic(err)
				}
				f.sourceFinishedCh <- item
				logger.Info("crawled", "url", item.GetURL(), "item", item.GetShortID())
				stats.SeedsFinishedIncr()
			}

			logger.Debug("item finished", "item", item.GetShortID())
		}
	}
}
