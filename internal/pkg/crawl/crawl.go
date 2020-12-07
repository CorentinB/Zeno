package crawl

import (
	"net/http"
	"sync"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/CorentinB/warc"
	"github.com/paulbellamy/ratecounter"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/remeh/sizedwaitgroup"
	"github.com/sirupsen/logrus"
	"mvdan.cc/xurls/v2"
)

var logInfo *logrus.Logger
var logWarning *logrus.Logger

// PrometheusMetrics define all the metrics exposed by the Prometheus exporter
type PrometheusMetrics struct {
	Prefix        string
	DownloadedURI prometheus.Counter
}

// Crawl define the parameters of a crawl process
type Crawl struct {
	*sync.Mutex
	StartTime time.Time
	SeedList  []frontier.Item
	Finished  *utils.TAtomBool

	// Frontier
	Frontier *frontier.Frontier

	// Crawl settings
	WorkerPool            sizedwaitgroup.SizedWaitGroup
	Client                *http.Client
	ClientProxied         *http.Client
	Logger                logrus.Logger
	DisabledHTMLTags      []string
	ExcludedHosts         []string
	UserAgent             string
	Job                   string
	JobPath               string
	MaxHops               uint8
	MaxRetry              int
	MaxRedirect           int
	CaptureAlternatePages bool
	DomainsCrawl          bool
	Headless              bool
	Seencheck             bool
	Workers               int

	// Proxy settings
	Proxy       string
	BypassProxy []string

	// API settings
	API               bool
	APIPort           string
	Prometheus        bool
	PrometheusMetrics *PrometheusMetrics

	// Real time statistics
	URIsPerSecond *ratecounter.RateCounter
	ActiveWorkers *ratecounter.Counter
	Crawled       *ratecounter.Counter

	// WARC settings
	WARC             bool
	WARCPrefix       string
	WARCOperator     string
	WARCWriter       chan *warc.RecordBatch
	WARCWriterFinish chan bool

	// Kafka settings
	UseKafka             bool
	KafkaBrokers         []string
	KafkaConsumerGroup   string
	KafkaFeedTopic       string
	KafkaOutlinksTopic   string
	KafkaProducerChannel chan *frontier.Item
}

// Start fire up the crawling process
func (c *Crawl) Start() (err error) {
	c.StartTime = time.Now()
	c.Finished = new(utils.TAtomBool)
	regexOutlinks = xurls.Relaxed()

	// Setup logging
	logInfo, logWarning = utils.SetupLogging(c.JobPath)

	// Initialize HTTP client
	c.initHTTPClient()

	// Start the background process that will handle os signals
	// to exit Zeno, like CTRL+C
	go c.setupCloseHandler()

	// Initialize the frontier
	c.Frontier.Init(c.JobPath, logInfo, logWarning, c.Workers, c.Seencheck)
	c.Frontier.Load()
	c.Frontier.Start()

	// Function responsible for writing to disk the frontier's hosts pool
	// and other stats needed to resume the crawl. The process happen every minute.
	// The actual queue used during the crawl and seencheck aren't included in this,
	// because they are written to disk in real-time.
	go c.writeFrontierToDisk()

	// Initialize WARC writer
	if c.WARC {
		logrus.Info("Initializing WARC writer pool..")
		c.initWARCWriter()
		logrus.Info("WARC writer pool initialized")
	}

	if c.API {
		go c.startAPI()
	}

	// Fire up the desired amount of workers
	for i := 0; i < c.Workers; i++ {
		c.WorkerPool.Add()
		go c.Worker(&c.WorkerPool)
	}

	// Start the process responsible for printing live stats on the standard output
	go c.printLiveStats()

	// If Kafka parameters are specified, then we start the background
	// processes responsible for pulling and pushing seeds from and to Kafka
	if c.UseKafka {
		c.KafkaProducerChannel = make(chan *frontier.Item, c.Workers)
		go c.kafkaConsumer()
		if len(c.KafkaOutlinksTopic) > 0 {
			go c.kafkaProducer()
		}
	} else {
		// Push the seed list to the queue
		logrus.Info("Pushing seeds in the local queue..")
		for _, item := range c.SeedList {
			item := item
			c.Frontier.PushChan <- &item
		}
		c.SeedList = nil
		logrus.Info("All seeds are now in queue, crawling will start")
	}

	// Start the background process that will catch when there
	// is nothing more to crawl
	if !c.UseKafka {
		c.catchFinish()
	} else {
		for {
			time.Sleep(time.Second)
		}
	}

	return
}
