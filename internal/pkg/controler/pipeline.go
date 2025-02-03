package controler

import (
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/archiver"
	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/controler/watchers"
	"github.com/internetarchive/Zeno/internal/pkg/finisher"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/postprocessor"
	"github.com/internetarchive/Zeno/internal/pkg/preprocessor"
	"github.com/internetarchive/Zeno/internal/pkg/preprocessor/seencheck"
	"github.com/internetarchive/Zeno/internal/pkg/reactor"
	"github.com/internetarchive/Zeno/internal/pkg/source/hq"
	"github.com/internetarchive/Zeno/internal/pkg/stats"
	"github.com/internetarchive/Zeno/pkg/models"
)

func startPipeline() {
	err := log.Start()
	if err != nil {
		fmt.Println("error starting logger", "err", err.Error())
		panic(err)
	}

	logger := log.NewFieldedLogger(&log.Fields{
		"component": "controler.StartPipeline",
	})

	err = stats.Init()
	if err != nil {
		logger.Error("error initializing stats", "err", err.Error())
		panic(err)
	}

	// Start the disk watcher
	go watchers.WatchDiskSpace(config.Get().JobPath, 5*time.Second)

	// Start the reactor that will receive
	reactorOutputChan := makeStageChannel()
	err = reactor.Start(config.Get().WorkersCount, reactorOutputChan)
	if err != nil {
		logger.Error("error starting reactor", "err", err.Error())
		panic(err)
	}

	// If needed, create the seencheck DB (only if not using HQ)
	if config.Get().UseSeencheck && !config.Get().UseHQ {
		err := seencheck.Start(config.Get().JobPath)
		if err != nil {
			logger.Error("unable to start seencheck", "err", err.Error())
			panic(err)
		}
	}

	preprocessorOutputChan := makeStageChannel()
	err = preprocessor.Start(reactorOutputChan, preprocessorOutputChan)
	if err != nil {
		logger.Error("error starting preprocessor", "err", err.Error())
		panic(err)
	}

	archiverOutputChan := makeStageChannel()
	err = archiver.Start(preprocessorOutputChan, archiverOutputChan)
	if err != nil {
		logger.Error("error starting archiver", "err", err.Error())
		panic(err)
	}

	// Start the WARC writing queue watcher
	go watchers.WatchWARCWritingQueue(5 * time.Second)

	postprocessorOutputChan := makeStageChannel()
	err = postprocessor.Start(archiverOutputChan, postprocessorOutputChan)
	if err != nil {
		logger.Error("error starting postprocessor", "err", err.Error())
		panic(err)
	}

	var finisherFinishChan, finisherProduceChan chan *models.Item
	if config.Get().UseHQ {
		logger.Info("starting hq")

		finisherFinishChan = makeStageChannel()
		finisherProduceChan = makeStageChannel()

		err = hq.Start(finisherFinishChan, finisherProduceChan)
		if err != nil {
			logger.Error("error starting hq source, retrying", "err", err.Error())
			panic(err)
		}
	}

	err = finisher.Start(postprocessorOutputChan, finisherFinishChan, finisherProduceChan)
	if err != nil {
		logger.Error("error starting finisher", "err", err.Error())
		panic(err)
	}

	// Pipe in the reactor the input seeds if any
	if len(config.Get().InputSeeds) > 0 {
		for _, seed := range config.Get().InputSeeds {
			parsedURL := &models.URL{Raw: seed}
			err := parsedURL.Parse()
			if err != nil {
				panic(err)
			}

			item := models.NewItem(uuid.New().String(), parsedURL, "", true)
			item.SetSource(models.ItemSourceQueue)

			err = reactor.ReceiveInsert(item)
			if err != nil {
				logger.Error("unable to insert seed", "err", err.Error())
				panic(err)
			}
		}
	}
}

func stopPipeline() {
	logger := log.NewFieldedLogger(&log.Fields{
		"component": "controler.stopPipeline",
	})

	watchers.StopDiskWatcher()
	watchers.StopWARCWritingQueueWatcher()

	reactor.Freeze()

	preprocessor.Stop()
	archiver.Stop()
	postprocessor.Stop()
	finisher.Stop()

	if config.Get().UseSeencheck && !config.Get().UseHQ {
		seencheck.Close()
	}

	if config.Get().UseHQ {
		hq.Stop()
	}

	reactor.Stop()

	if config.Get().WARCTempDir != "" {
		err := os.Remove(config.Get().WARCTempDir)
		if err != nil {
			logger.Error("unable to remove temp dir", "err", err.Error())
		}
	}

	logger.Info("done, logs are flushing and will be closed")

	log.Stop()
}
