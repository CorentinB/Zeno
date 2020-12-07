package crawl

import (
	"fmt"
	"os"
	"time"

	"github.com/gin-contrib/pprof"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func (crawl *Crawl) startAPI() {
	gin.SetMode(gin.ReleaseMode)
	gin.DefaultWriter = logInfo.Out

	r := gin.Default()

	pprof.Register(r)

	logInfo.Info("Starting API")
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"rate":         crawl.URIsPerSecond.Rate(),
			"crawled":      crawl.Crawled.Value(),
			"queued":       crawl.Frontier.QueueCount.Value(),
			"running_time": fmt.Sprintf("%s", time.Since(crawl.StartTime)),
		})
	})

	// Handle Prometheus export
	if crawl.Prometheus {
		labels := make(map[string]string)

		labels["crawljob"] = crawl.Job
		hostname, err := os.Hostname()
		if err != nil {
			logWarning.Warn("Unable to retrieve hostname of machine")
			hostname = "unknown"
		}
		labels["host"] = hostname + ":" + crawl.APIPort

		crawl.PrometheusMetrics.DownloadedURI = promauto.NewCounter(prometheus.CounterOpts{
			Name:        crawl.PrometheusMetrics.Prefix + "downloaded_uri_count_total",
			ConstLabels: labels,
			Help:        "The total number of crawled URI",
		})

		logInfo.Info("Starting Prometheus export")
		r.GET("/metrics", gin.WrapH(promhttp.Handler()))
	}

	r.Run(":" + crawl.APIPort)
}
