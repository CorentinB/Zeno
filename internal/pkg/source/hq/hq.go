package hq

import (
	"context"
	"sync"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
	"github.com/internetarchive/gocrawlhq"
)

type hq struct {
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	finishCh  chan *models.Item
	produceCh chan *models.Item
	client    *gocrawlhq.Client
}

var (
	globalHQ *hq
	once     sync.Once
	logger   *log.FieldedLogger
)

func Start(finishChan, produceChan chan *models.Item) error {
	var done bool
	var startErr error

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "hq",
	})

	stats.Init()

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		HQclient, err := gocrawlhq.Init(config.Get().HQKey, config.Get().HQSecret, config.Get().HQProject, config.Get().HQAddress, "")
		if err != nil {
			logger.Error("error initializing crawl HQ client", "err", err.Error(), "func", "hq.Start")
			startErr = err
		}

		globalHQ = &hq{
			wg:        sync.WaitGroup{},
			ctx:       ctx,
			cancel:    cancel,
			finishCh:  finishChan,
			produceCh: produceChan,
			client:    HQclient,
		}

		globalHQ.wg.Add(3)
		go consumer()
		go producer()
		go finisher()

		logger.Info("started")

		done = true
	})

	if !done {
		return ErrHQAlreadyInitialized
	}

	return startErr
}

func Stop() {
	if globalHQ != nil {
		globalHQ.cancel()
		globalHQ.wg.Wait()
		if err := globalHQ.client.Reset(); err != nil {
			logger.Error("error while reseting", "err", err)
		}
		once = sync.Once{}
		logger.Info("stopped")
	}
}
