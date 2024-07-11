package crawl

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/CorentinB/warc"
	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// APIWorkersState represents the state of all API workers.
type APIWorkersState struct {
	Workers []*APIWorkerState `json:"workers"`
}

// APIWorkerState represents the state of an API worker.
type APIWorkerState struct {
	WorkerID  uint   `json:"worker_id"`
	Status    string `json:"status"`
	LastError string `json:"last_error"`
	LastSeen  string `json:"last_seen"`
	Locked    bool   `json:"locked"`
}

// startAPI starts the API server for the crawl.
func (crawl *Crawl) startAPI() {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = crawl.Log.Writer(slog.LevelInfo)
	gin.DefaultErrorWriter = crawl.Log.Writer(slog.LevelError)

	r := gin.Default()

	pprof.Register(r)

	crawl.Log.Info("Starting API")
	r.GET("/", func(c *gin.Context) {
		crawledSeeds := crawl.CrawledSeeds.Value()
		crawledAssets := crawl.CrawledAssets.Value()

		c.JSON(200, gin.H{
			"rate":                crawl.URIsPerSecond.Rate(),
			"crawled":             crawledSeeds + crawledAssets,
			"crawled_seeds":       crawledSeeds,
			"crawled_assets":      crawledAssets,
			"queued":              crawl.Frontier.QueueCount.Value(),
			"data_written":        warc.DataTotal.Value(),
			"data_deduped_remote": warc.RemoteDedupeTotal.Value(),
			"data_deduped_local":  warc.LocalDedupeTotal.Value(),
			"uptime":              time.Since(crawl.StartTime).String(),
		})
	})

	// Handle Prometheus export
	if crawl.Prometheus {
		labels := make(map[string]string)

		labels["crawljob"] = crawl.Job
		hostname, err := os.Hostname()
		if err != nil {
			crawl.Log.Warn("Unable to retrieve hostname of machine")
			hostname = "unknown"
		}
		labels["host"] = hostname + ":" + crawl.APIPort

		crawl.PrometheusMetrics.DownloadedURI = promauto.NewCounter(prometheus.CounterOpts{
			Name:        crawl.PrometheusMetrics.Prefix + "downloaded_uri_count_total",
			ConstLabels: labels,
			Help:        "The total number of crawled URI",
		})

		crawl.Log.Info("Starting Prometheus export")
		r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	r.GET("/workers", func(c *gin.Context) {
		workersState := crawl.GetWorkerState(-1)
		c.JSON(200, workersState)
	})

	r.GET("/worker/:worker_id", func(c *gin.Context) {
		workerID := c.Param("worker_id")
		workerIDInt, err := strconv.Atoi(workerID)
		if err != nil {
			c.JSON(400, gin.H{
				"error": "Unsupported worker ID",
			})
			return
		}

		workersState := crawl.GetWorkerState(workerIDInt)
		if workersState == nil {
			c.JSON(404, gin.H{
				"error": "Worker not found",
			})
			return
		}

		c.JSON(200, workersState)
	})

	err := r.Run(fmt.Sprintf(":%s", crawl.APIPort))
	if err != nil {
		crawl.Log.Fatal("unable to start API", "error", err.Error())
	}
}
