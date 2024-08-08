package crawl

import (
	"encoding/json"
	"os"
	"strings"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/internetarchive/Zeno/internal/stats"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// startAPI starts the API server for the crawl
func (crawl *Crawl) startAPI() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		crawledSeeds := stats.GetCrawledSeeds()
		crawledAssets := stats.GetCrawledAssets()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)

		response := map[string]interface{}{
			"rate":          stats.GetURIPerSecond(),
			"crawled":       crawledSeeds + crawledAssets,
			"crawledSeeds":  crawledSeeds,
			"crawledAssets": crawledAssets,
			"queued":        stats.GetQueueTotalElementsCount(),
			"uptime":        time.Since(crawl.StartTime).String(),
		}

		json.NewEncoder(w).Encode(response)
	})

	if crawl.Prometheus {
		http.HandleFunc("/metrics", setupPrometheus(crawl).ServeHTTP)
	}

	http.HandleFunc("/queue", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(stats.GetJSONQueueStats())
	})

	http.HandleFunc("/workers", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workersState := crawl.Workers.GetWorkerStateFromPool("")
		json.NewEncoder(w).Encode(workersState)
	})

	http.HandleFunc("/worker/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		workerID := strings.TrimPrefix(r.URL.Path, "/worker/")
		workersState := crawl.Workers.GetWorkerStateFromPool(workerID)
		if workersState == nil {
			w.WriteHeader(http.StatusNotFound)
			json.NewEncoder(w).Encode(map[string]interface{}{
				"error": "Worker not found",
			})
			return
		}

		json.NewEncoder(w).Encode(workersState)
	})

	err := http.ListenAndServe(":"+crawl.APIPort, nil)
	if err != nil {
		crawl.Log.Fatal("unable to start API", "error", err.Error())
	}
}

func setupPrometheus(crawl *Crawl) http.Handler {
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

	crawl.Log.Info("starting Prometheus export")

	return promhttp.Handler()
}
