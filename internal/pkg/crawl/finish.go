package crawl

import (
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// catchFinish is running in the background and detect when the crawl need to be terminated
// because it won't crawl anything more. This doesn't apply for Kafka-powered crawls.
func (crawl *Crawl) catchFinish() {
	for crawl.Crawled.Value() <= 0 {
		time.Sleep(1 * time.Second)
	}

	for {
		time.Sleep(time.Second * 5)
		if crawl.ActiveWorkers.Value() == 0 && crawl.Frontier.QueueCount.Value() == 0 && crawl.Finished.Get() == false && crawl.Crawled.Value() > 0 {
			logrus.Warning("No additional URL to archive, finishing")
			crawl.finish()
			os.Exit(0)
		}
	}
}

func (crawl *Crawl) finish() {
	crawl.Finished.Set(true)

	// First we wait for the queue reader to finish its current work,
	// and stop it, when it's stopped it won't dispatch any additional work
	// so we can safely close the channel it is using, and wait for all the
	// workers to notice the channel is closed, and terminate.
	crawl.Frontier.FinishingQueueReader.Set(true)
	for crawl.Frontier.IsQueueReaderActive.Get() != false {
		time.Sleep(time.Second)
	}
	close(crawl.Frontier.PullChan)

	crawl.WorkerPool.Wait()
	logrus.Warning("All workers finished")

	// Once all workers are done, it means nothing more is actively send to
	// the PushChan channel, we ask for the queue writer to terminate, and when
	// it's done we close the channel safely.
	crawl.Frontier.FinishingQueueWriter.Set(true)
	close(crawl.Frontier.PushChan)
	for crawl.Frontier.IsQueueWriterActive.Get() != false {
		time.Sleep(time.Second)
	}

	// Closing the WARC writing channel
	if crawl.WARC {
		close(crawl.WARCWriter)
		<-crawl.WARCWriterFinish
		close(crawl.WARCWriterFinish)
		logrus.Warning("WARC writer closed")
	}

	// Closing the local queue used by the frontier
	crawl.Frontier.Queue.Close()
	logrus.Warning("Frontier queue closed")

	// Closing the seencheck database
	if crawl.Seencheck {
		crawl.Frontier.Seencheck.SeenDB.Close()
		logrus.Warning("Seencheck database closed")
	}

	// Dumping hosts pool and frontier stats to disk
	logrus.Warning("Dumping hosts pool and frontier stats to " + path.Join(crawl.Frontier.JobPath, "frontier.gob"))
	crawl.Frontier.Save()

	logrus.Warning("Finished")
}

func (crawl *Crawl) setupCloseHandler() {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	logrus.Warning("CTRL+C catched.. cleaning up and exiting.")
	signal.Stop(c)
	close(c)
	crawl.finish()
	os.Exit(0)
}
