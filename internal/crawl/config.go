package crawl

import (
	"log/slog"
	"net/http"
	"path"
	"path/filepath"
	"sync"
	"time"

	"github.com/CorentinB/warc"
	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/config"
	"github.com/internetarchive/Zeno/internal/item"
	"github.com/internetarchive/Zeno/internal/log"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/seencheck"
	"github.com/internetarchive/Zeno/internal/utils"
	"github.com/internetarchive/Zeno/internal/worker"
)

// Crawl define the parameters of a crawl process
type Crawl struct {
	*sync.Mutex
	StartTime time.Time
	SeedList  []item.Item
	Paused    *utils.TAtomBool
	Finished  *utils.TAtomBool

	// Stats
	UseLiveStats             bool
	stopMonitorWARCWaitGroup chan struct{}

	// Logger
	Log *log.Logger

	// Queue (ex-frontier)
	Queue        *queue.PersistentGroupedQueue
	Seencheck    *seencheck.Seencheck
	UseSeencheck bool
	UseHandover  bool
	UseCommit    bool

	// Worker pool
	Workers *worker.Pool

	// Crawl settings
	MaxConcurrentAssets            int
	Client                         *warc.CustomHTTPClient
	ClientProxied                  *warc.CustomHTTPClient
	DisabledHTMLTags               []string
	ExcludedHosts                  []string
	IncludedHosts                  []string
	ExcludedStrings                []string
	UserAgent                      string
	Job                            string
	JobPath                        string
	MaxHops                        uint8
	MaxRetry                       uint8
	MaxRedirect                    uint8
	HTTPTimeout                    int
	MaxConcurrentRequestsPerDomain int
	RateLimitDelay                 int
	CrawlTimeLimit                 int
	MaxCrawlTimeLimit              int
	DisableAssetsCapture           bool
	CaptureAlternatePages          bool
	DomainsCrawl                   bool
	Headless                       bool
	RandomLocalIP                  bool
	MinSpaceRequired               int

	// Cookie-related settings
	CookieFile  string
	KeepCookies bool
	CookieJar   http.CookieJar

	// proxy settings
	Proxy       string
	BypassProxy []string

	// API settings
	API               bool
	APIPort           string
	Prometheus        bool
	PrometheusMetrics *PrometheusMetrics

	// WARC settings
	WARCPrefix         string
	WARCOperator       string
	WARCWriter         chan *warc.RecordBatch
	WARCWriterFinish   chan bool
	WARCTempDir        string
	CDXDedupeServer    string
	WARCFullOnDisk     bool
	WARCPoolSize       int
	WARCDedupSize      int
	DisableLocalDedupe bool
	CertValidation     bool
	WARCCustomCookie   string

	// Crawl HQ settings
	UseHQ            bool
	HQAddress        string
	HQProject        string
	HQKey            string
	HQSecret         string
	HQStrategy       string
	HQBatchSize      int
	HQContinuousPull bool
	// HQClient               *gocrawlhq.Client // Moved to hq package
	HQFinishedChannel      chan *item.Item
	HQProducerChannel      chan *item.Item
	HQChannelsWg           *sync.WaitGroup
	HQRateLimitingSendBack bool
}

func GenerateCrawlConfig(config *config.Config) (*Crawl, error) {
	var c = new(Crawl)

	var logfileOutputDir string
	if config.LogFileOutputDir == "" {
		if config.Job != "" {
			logfileOutputDir = path.Join("jobs", config.Job, "logs")
		} else {
			logfileOutputDir = path.Join("jobs", "logs")
		}
	} else {
		logfileOutputDir = filepath.Clean(config.LogFileOutputDir)
	}

	// Logger
	customLoggerConfig := log.Config{
		FileConfig: &log.LogfileConfig{
			Dir:    logfileOutputDir,
			Prefix: "zeno",
		},
		FileLevel:                slog.LevelDebug,
		StdoutEnabled:            !config.NoStdoutLogging,
		StdoutLevel:              slog.LevelInfo,
		RotateLogFile:            true,
		RotateElasticSearchIndex: true,
		ElasticsearchConfig: &log.ElasticsearchConfig{
			Addresses:   config.ElasticSearchURLs,
			Username:    config.ElasticSearchUsername,
			Password:    config.ElasticSearchPassword,
			IndexPrefix: config.ElasticSearchIndexPrefix,
			Level:       slog.LevelDebug,
		},
	}
	if len(config.ElasticSearchURLs) == 0 || (config.ElasticSearchUsername == "" && config.ElasticSearchPassword == "") {
		customLoggerConfig.ElasticsearchConfig = nil
	}

	customLogger, err := log.New(customLoggerConfig)
	if err != nil {
		return nil, err
	}
	c.Log = customLogger

	// Stats
	c.UseLiveStats = config.LiveStats
	c.stopMonitorWARCWaitGroup = make(chan struct{})

	// If the job name isn't specified, we generate a random name
	if config.Job == "" {
		if config.HQProject != "" {
			c.Job = config.HQProject
		} else {
			UUID, err := uuid.NewUUID()
			if err != nil {
				c.Log.Error("cmd/utils.go:InitCrawlWithCMD():uuid.NewUUID()", "error", err)
				return nil, err
			}

			c.Job = UUID.String()
		}
	} else {
		c.Job = config.Job
	}

	c.JobPath = path.Join("jobs", config.Job)

	c.Workers = worker.NewPool(uint(config.WorkersCount), time.Second*60)

	c.UseSeencheck = config.LocalSeencheck
	c.HTTPTimeout = config.HTTPTimeout
	c.MaxConcurrentRequestsPerDomain = config.MaxConcurrentRequestsPerDomain
	c.RateLimitDelay = config.ConcurrentSleepLength
	c.CrawlTimeLimit = config.CrawlTimeLimit

	// Defaults --max-crawl-time-limit to 10% more than --crawl-time-limit
	if config.CrawlMaxTimeLimit == 0 && config.CrawlTimeLimit != 0 {
		c.MaxCrawlTimeLimit = config.CrawlTimeLimit + (config.CrawlTimeLimit / 10)
	} else {
		c.MaxCrawlTimeLimit = config.CrawlMaxTimeLimit
	}

	c.MaxRetry = config.MaxRetry
	c.MaxRedirect = config.MaxRedirect
	c.MaxHops = config.MaxHops
	c.DomainsCrawl = config.DomainsCrawl
	c.DisableAssetsCapture = config.DisableAssetsCapture
	c.DisabledHTMLTags = config.DisableHTMLTag
	c.ExcludedHosts = config.ExcludeHosts
	c.IncludedHosts = config.IncludeHosts
	c.CaptureAlternatePages = config.CaptureAlternatePages
	c.ExcludedStrings = config.ExcludeString

	c.MinSpaceRequired = config.MinSpaceRequired

	// WARC settings
	c.WARCPrefix = config.WARCPrefix
	c.WARCOperator = config.WARCOperator

	if config.WARCTempDir != "" {
		c.WARCTempDir = config.WARCTempDir
	} else {
		c.WARCTempDir = path.Join(c.JobPath, "temp")
	}

	c.CDXDedupeServer = config.CDXDedupeServer
	c.DisableLocalDedupe = config.DisableLocalDedupe
	c.CertValidation = config.CertValidation
	c.WARCFullOnDisk = config.WARCOnDisk
	c.WARCPoolSize = config.WARCPoolSize
	c.WARCDedupSize = config.WARCDedupeSize
	c.WARCCustomCookie = config.CDXCookie

	c.API = config.API
	c.APIPort = config.APIPort

	// If Prometheus is specified, then we make sure
	// c.API is true
	c.Prometheus = config.Prometheus
	if c.Prometheus {
		c.API = true
		c.PrometheusMetrics = &PrometheusMetrics{}
		c.PrometheusMetrics.Prefix = config.PrometheusPrefix
	}

	if config.UserAgent != "Zeno" {
		c.UserAgent = config.UserAgent
	} else {
		version := utils.GetVersion()

		// If Version is a commit hash, we only take the first 7 characters
		if len(version.Version) == 40 {
			version.Version = version.Version[:7]
		}

		c.UserAgent = "Mozilla/5.0 (compatible; archive.org_bot +http://archive.org/details/archive.org_bot) Zeno/" + version.Version + " warc/" + version.WarcVersion
	}
	c.Headless = config.Headless

	c.CookieFile = config.Cookies
	c.KeepCookies = config.KeepCookies

	// Proxy settings
	c.Proxy = config.Proxy
	c.BypassProxy = config.DomainsBypassProxy

	// Crawl HQ settings
	c.UseHQ = config.HQ
	c.HQProject = config.HQProject
	c.HQAddress = config.HQAddress
	c.HQKey = config.HQKey
	c.HQSecret = config.HQSecret
	c.HQStrategy = config.HQStrategy
	c.HQBatchSize = int(config.HQBatchSize)
	c.HQContinuousPull = config.HQContinuousPull
	c.HQRateLimitingSendBack = config.HQRateLimitSendBack

	// Handover mechanism
	c.UseHandover = !config.NoHandover

	c.UseCommit = !config.NoBatchWriteWAL

	return c, nil
}
