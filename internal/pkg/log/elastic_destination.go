package log

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	elastic "github.com/elastic/go-elasticsearch/v7"
)

// ElasticsearchDestination logs to Elasticsearch
type ElasticsearchDestination struct {
	level     slog.Level
	config    *ElasticsearchConfig
	client    *elastic.Client
	index     string
	mu        sync.Mutex
	ticker    *time.Ticker
	closeChan chan struct{}
}

func NewElasticsearchDestination(cfg *ElasticsearchConfig) *ElasticsearchDestination {
	es, err := elastic.NewClient(elastic.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
	})
	if err != nil {
		// Handle error (for simplicity, we'll just ignore it here)
		return nil
	}

	ed := &ElasticsearchDestination{
		level:     cfg.Level,
		config:    cfg,
		client:    es,
		index:     cfg.IndexPrefix + "-" + time.Now().Format("2006.01.02"),
		closeChan: make(chan struct{}),
	}

	if config.RotateElasticSearchIndex && config.RotatePeriod > 0 {
		ed.ticker = time.NewTicker(config.RotatePeriod)
		go ed.rotationWorker()
	}

	return ed
}

func (d *ElasticsearchDestination) Enabled() bool {
	return d.client != nil
}

func (d *ElasticsearchDestination) Level() slog.Level {
	return d.level
}

func (d *ElasticsearchDestination) Write(entry *logEntry) {
	doc := map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"level":     entry.level.String(),
		"message":   entry.msg,
		"fields":    entry.args,
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(doc); err != nil {
		// Handle error
		return
	}

	req := esapi.IndexRequest{
		Index:      d.index,
		DocumentID: "", // Auto-generate ID
		Body:       &buf,
		Refresh:    "true",
	}

	_, err := req.Do(context.Background(), d.client)
	if err != nil {
		// Handle error
	}
}

func (d *ElasticsearchDestination) Close() {
	if d.ticker != nil {
		d.ticker.Stop()
	}
	close(d.closeChan)
}

func (d *ElasticsearchDestination) rotationWorker() {
	for {
		select {
		case <-d.ticker.C:
			d.mu.Lock()
			d.index = d.config.IndexPrefix + "-" + time.Now().Format("2006.01.02")
			d.mu.Unlock()
		case <-d.closeChan:
			return
		}
	}
}
