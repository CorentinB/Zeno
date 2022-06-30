package utils

import (
	"fmt"
	"io"
	"os"
	"path"
	"time"

	rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	"github.com/sirupsen/logrus"
)

// SetupLogging setup the logger for the crawl
func SetupLogging(jobPath string, liveStats bool) (logInfo, logWarning *logrus.Logger) {
	var logsDirectory = path.Join(jobPath, "logs")
	logInfo = logrus.New()
	logWarning = logrus.New()

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

	if !liveStats {
		logInfo.SetOutput(io.MultiWriter(writerInfo, os.Stdout))
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

	if !liveStats {
		logWarning.SetOutput(io.MultiWriter(writerWarning, os.Stdout))
	} else {
		logWarning.SetOutput(writerWarning)
	}

	return logInfo, logWarning
}
