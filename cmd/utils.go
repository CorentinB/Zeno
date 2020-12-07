package cmd

import (
	"path"
	"time"

	"github.com/CorentinB/Zeno/config"
	"github.com/CorentinB/Zeno/internal/pkg/crawl"
	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/google/uuid"
	"github.com/paulbellamy/ratecounter"
	"github.com/remeh/sizedwaitgroup"
	"github.com/sirupsen/logrus"
)

// InitCrawlWithCMD takes a config.Flags struct and return a
// *crawl.Crawl initialized with it
func InitCrawlWithCMD(flags config.Flags) *crawl.Crawl {
	var c = new(crawl.Crawl)

	// Statistics counters
	c.Crawled = new(ratecounter.Counter)
	c.ActiveWorkers = new(ratecounter.Counter)
	c.URIsPerSecond = ratecounter.NewRateCounter(1 * time.Second)

	// Frontier
	c.Frontier = new(frontier.Frontier)

	// If the job name isn't specified, we generate a random name
	if len(flags.Job) == 0 {
		UUID, err := uuid.NewUUID()
		if err != nil {
			logrus.Fatal(err)
		}
		c.Job = UUID.String()
	} else {
		c.Job = flags.Job
	}
	c.JobPath = path.Join("jobs", flags.Job)

	c.Workers = flags.Workers
	c.WorkerPool = sizedwaitgroup.New(c.Workers)

	c.Seencheck = flags.Seencheck
	c.MaxRetry = flags.MaxRetry
	c.MaxRedirect = flags.MaxRedirect
	c.MaxHops = uint8(flags.MaxHops)
	c.DomainsCrawl = flags.DomainsCrawl
	c.DisabledHTMLTags = flags.DisabledHTMLTags.Value()
	c.ExcludedHosts = flags.ExcludedHosts.Value()
	c.CaptureAlternatePages = flags.CaptureAlternatePages

	// WARC settings
	c.WARC = flags.WARC
	c.WARCPrefix = flags.WARCPrefix
	c.WARCOperator = flags.WARCOperator

	c.API = flags.API
	c.APIPort = flags.APIPort

	// If Prometheus is specified, then we make sure
	// c.API is true
	c.Prometheus = flags.Prometheus
	if c.Prometheus == true {
		c.API = true
		c.PrometheusMetrics = new(crawl.PrometheusMetrics)
		c.PrometheusMetrics.Prefix = flags.PrometheusPrefix
	}

	c.UserAgent = flags.UserAgent
	c.Headless = flags.Headless

	// Proxy settings
	c.Proxy = flags.Proxy
	c.BypassProxy = flags.BypassProxy.Value()

	// Kafka settings
	c.UseKafka = flags.Kafka
	c.KafkaConsumerGroup = flags.KafkaConsumerGroup
	c.KafkaFeedTopic = flags.KafkaFeedTopic
	c.KafkaOutlinksTopic = flags.KafkaOutlinksTopic
	c.KafkaBrokers = flags.KafkaBrokers.Value()
	if c.UseKafka && (len(c.KafkaFeedTopic) == 0 || len(c.KafkaBrokers) == 0) {
		logrus.Fatal("Kafka used but no broker or feed topic specified")
	}

	return c
}
