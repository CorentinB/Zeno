// Zeno is a web crawler designed to operate wide crawls or to simply archive one web page.
// Zeno's key concepts are: portability, performance, simplicity ; with an emphasis on performance.

// Authors:
//
//	Corentin Barreau <corentin@archive.org>
//	Jake LaFountain <jakelf@archive.org>
//	Thomas Foubert <thomas@archive.org>
package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/internetarchive/Zeno/cmd"
	"github.com/internetarchive/Zeno/internal/pkg/archiver"
	"github.com/internetarchive/Zeno/internal/pkg/config"
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

var (
	logger *log.FieldedLogger
)

func main() {
	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "main",
	})
	defer log.Stop()

	if err := cmd.Run(); err != nil {
		logger.Error("unable to run root command", "err", err.Error())
		return
	}

	stats.Init()

	// If needed, start the seencheck process
	if config.Get().UseSeencheck {
		err := seencheck.Start(config.Get().JobPath)
		if err != nil {
			logger.Error("unable to start seencheck", "err", err.Error())
			return
		}
	}

	// Start the reactor that will receive
	reactorOutputChan := make(chan *models.Item)
	err := reactor.Start(config.Get().WorkersCount, reactorOutputChan)

	preprocessorOutputChan := make(chan *models.Item)
	err = preprocessor.Start(reactorOutputChan, preprocessorOutputChan)
	if err != nil {
		logger.Error("error starting preprocessor", "err", err.Error())
		return
	}

	archiverOutputChan := make(chan *models.Item)
	err = archiver.Start(preprocessorOutputChan, archiverOutputChan)
	if err != nil {
		logger.Error("error starting archiver", "err", err.Error())
		return
	}

	postprocessorOutputChan := make(chan *models.Item)
	err = postprocessor.Start(archiverOutputChan, postprocessorOutputChan)
	if err != nil {
		logger.Error("error starting postprocessor", "err", err.Error())
		return
	}

	hqFinishChan := make(chan *models.Item)
	hqProduceChan := make(chan *models.Item)
	err = hq.Start(hqFinishChan, hqProduceChan)
	if err != nil {
		logger.Error("error starting hq", "err", err.Error())
		return
	}

	err = finisher.Start(postprocessorOutputChan, hqFinishChan, hqProduceChan)
	if err != nil {
		logger.Error("error starting finisher", "err", err.Error())
		return
	}

	// Handle OS signals for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signalChan:
		logger.Info("received shutdown signal, stopping services...")
		// Catch a second signal to force exit
		go func() {
			<-signalChan
			logger.Info("received second shutdown signal, forcing exit...")
			os.Exit(1)
		}()
	}

	finisher.Stop()
	postprocessor.Stop()
	archiver.Stop()
	preprocessor.Stop()
	reactor.Freeze()
	hq.Stop()
	reactor.Stop()

	close(reactorOutputChan)
	close(preprocessorOutputChan)
	close(archiverOutputChan)
	close(postprocessorOutputChan)
	close(hqFinishChan)
	close(hqProduceChan)

	logger.Info("all services stopped, exiting")
	return
}
