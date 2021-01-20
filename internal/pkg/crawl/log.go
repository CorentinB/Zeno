package crawl

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

// SetupLogging setup the logger for the crawl
func (c *Crawl) SetupLogging() (logInfo, logWarning *logrus.Logger) {
	var logsDirectory = path.Join(c.JobPath, "logs")
	logInfo = logrus.New()
	logWarning = logrus.New()

	if c.JSONLog {
		logInfo.SetFormatter(&logrus.JSONFormatter{})
		logWarning.SetFormatter(&logrus.JSONFormatter{})
	}

	if c.Debug {
		logInfo.SetReportCaller(true)
		logWarning.SetReportCaller(true)
	}

	// Create logs directory for the job
	os.MkdirAll(logsDirectory, os.ModePerm)

	// Initialize rotating loggers
	pathInfo := path.Join(logsDirectory, "zeno_info")
	pathWarning := path.Join(logsDirectory, "zeno_warning")

	writerInfo, err := rotatelogs.New(
		fmt.Sprintf("%s_%s.log", pathInfo, "%Y%m%d%H%M%S"),
		rotatelogs.WithRotationTime(time.Hour*6),
	)
	if err != nil {
		logrus.WithFields(logrus.Fields{"error": err}).Fatalln("Failed to initialize info log file")
	}

	if !c.LiveStats {
		infoMultiWriter := io.MultiWriter(os.Stdout, writerInfo)
		logInfo.SetOutput(infoMultiWriter)
	} else {
		logInfo.SetOutput(writerInfo)
	}

	writerWarning, err := rotatelogs.New(
		fmt.Sprintf("%s_%s.log", pathWarning, "%Y%m%d%H%M%S"),
		rotatelogs.WithRotationTime(time.Hour*6),
	)
	if err != nil {
		logrus.WithFields(logrus.Fields{"error": err}).Fatalln("Failed to initialize warning log file")
	}

	if !c.LiveStats {
		warnMultiWriter := io.MultiWriter(os.Stdout, writerWarning)
		logWarning.SetOutput(warnMultiWriter)
	} else {
		logWarning.SetOutput(writerWarning)
	}

	return logInfo, logWarning
}

func (c *Crawl) logCrawlSuccess(executionStart time.Time, statusCode int, item *frontier.Item) {
	logInfo.WithFields(logrus.Fields{
		"status":         c.getCrawlState(),
		"queued":         c.Frontier.QueueCount.Value(),
		"crawled":        c.Crawled.Value(),
		"rate":           c.URIsPerSecond.Rate(),
		"status_code":    statusCode,
		"active_workers": c.ActiveWorkers.Value(),
		"hop":            item.Hop,
		"type":           item.Type,
		"execution_time": time.Since(executionStart),
	}).Info(item.URL.String())
}
