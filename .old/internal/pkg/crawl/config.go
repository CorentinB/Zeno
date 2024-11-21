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
	"github.com/internetarchive/Zeno/config"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/seencheck"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
	"github.com/internetarchive/gocrawlhq"
	"github.com/paulbellamy/ratecounter"
)

// Crawl define the parameters of a crawl process
type Crawl struct {
	*sync.Mutex
	StartTime time.Time
	SeedList  []queue.Item
	Paused    *utils.TAtomBool
	Finished  *utils.TAtomBool
	LiveStats bool

	// Logger
	Log *log.Logger

	// Queue (ex-frontier)
	Queue        *queue.PersistentGroupedQueue
	Seencheck    *seencheck.Seencheck
	UseSeencheck bool
	UseHandover  bool
	UseCommit    bool

	// Worker pool
	Workers *WorkerPool

	// Crawl settings
	MaxConcurrentAssets            int
	Client                         *warc.CustomHTTPClient
	ClientProxied                  *warc.CustomHTTPClient
	DisabledHTMLTags               []string
	ExcludedHosts                  []string
	IncludedHosts                  []string
	IncludedStrings                []string
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
	MinSpaceRequired               int

	// Cookie-related settings
	CookieFile  string
	KeepCookies bool
	CookieJar   http.CookieJar

	// Network settings
	Proxy         string
	BypassProxy   []string
	RandomLocalIP bool
	DisableIPv4   bool
	DisableIPv6   bool
	IPv6AnyIP     bool

	// API settings
	API               bool
	APIPort           string
	Prometheus        bool
	PrometheusMetrics *PrometheusMetrics

	// Real time statistics
	URIsPerSecond *ratecounter.RateCounter
	ActiveWorkers *ratecounter.Counter
	CrawledSeeds  *ratecounter.Counter
	CrawledAssets *ratecounter.Counter

	// WARC settings
	WARCPrefix         string
	WARCOperator       string
	WARCWriter         chan *warc.RecordBatch
	WARCWriterFinish   chan bool
	WARCTempDir        string
	CDXDedupeServer    string
	WARCFullOnDisk     bool
	WARCPoolSize       int
	WARCDedupeSize     int
	WARCSize           int
	DisableLocalDedupe bool
	CertValidation     bool
	WARCCustomCookie   string

	// Crawl HQ settings
	UseHQ                  bool
	HQAddress              string
	HQProject              string
	HQKey                  string
	HQSecret               string
	HQStrategy             string
	HQBatchConcurrency     int
	HQBatchSize            int
	HQClient               *gocrawlhq.Client
	HQFinishedChannel      chan *queue.Item
	HQProducerChannel      chan *queue.Item
	HQChannelsWg           *sync.WaitGroup
	HQRateLimitingSendBack bool

	// Dependencies
	NoYTDLP   bool
	YTDLPPath string
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

	var fileLogLevel slog.Level
	if config.Debug {
		fileLogLevel = slog.LevelDebug
	} else {
		fileLogLevel = slog.LevelInfo
	}

	// Logger
	customLoggerConfig := log.Config{
		FileConfig: &log.LogfileConfig{
			Dir:    logfileOutputDir,
			Prefix: "zeno",
		},
		FileLevel:                fileLogLevel,
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

	// Statistics counters
	c.CrawledSeeds = new(ratecounter.Counter)
	c.CrawledAssets = new(ratecounter.Counter)
	c.ActiveWorkers = new(ratecounter.Counter)
	c.URIsPerSecond = ratecounter.NewRateCounter(1 * time.Second)

	c.LiveStats = config.LiveStats

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

	c.Workers = NewPool(uint(config.WorkersCount), time.Second*60, c)

	c.UseSeencheck = !config.DisableSeencheck
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

	// We exclude some hosts by default
	c.ExcludedHosts = utils.DedupeStrings(append(config.ExcludeHosts, "archive.org", "archive-it.org"))

	c.IncludedHosts = config.IncludeHosts
	c.CaptureAlternatePages = config.CaptureAlternatePages
	c.ExcludedStrings = config.ExcludeString
	c.IncludedStrings = config.IncludeString

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
	c.WARCDedupeSize = config.WARCDedupeSize
	c.WARCCustomCookie = config.CDXCookie
	c.WARCSize = config.WARCSize

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

	// Dependencies
	c.NoYTDLP = config.NoYTDLP
	c.YTDLPPath = config.YTDLPPath

	if config.UserAgent != "Zeno" {
		c.UserAgent = config.UserAgent
	} else {
		version := utils.GetVersion()

		// If Version is a commit hash, we only take the first 7 characters
		if len(version.Version) >= 40 {
			version.Version = version.Version[:7]
		}

		c.UserAgent = "Mozilla/5.0 (compatible; archive.org_bot +http://archive.org/details/archive.org_bot) Zeno/" + version.Version + " warc/" + version.WarcVersion
		c.Log.Info("User-Agent set to", "user-agent", c.UserAgent)
	}
	c.Headless = config.Headless

	c.CookieFile = config.Cookies
	c.KeepCookies = config.KeepCookies

	// Network settings
	c.Proxy = config.Proxy
	c.BypassProxy = config.DomainsBypassProxy
	c.RandomLocalIP = config.RandomLocalIP

	if c.RandomLocalIP {
		c.Log.Warn("Random local IP is enabled")
	}

	c.DisableIPv4 = config.DisableIPv4
	c.DisableIPv6 = config.DisableIPv6
	c.IPv6AnyIP = config.IPv6AnyIP

	if c.DisableIPv4 && c.DisableIPv6 {
		c.Log.Fatal("Both IPv4 and IPv6 are disabled, at least one of them must be enabled.")
	} else if c.DisableIPv4 {
		c.Log.Info("IPv4 is disabled")
	} else if c.DisableIPv6 {
		c.Log.Info("IPv6 is disabled")
	}

	// Crawl HQ settings
	c.UseHQ = config.HQ
	c.HQProject = config.HQProject
	c.HQAddress = config.HQAddress
	c.HQKey = config.HQKey
	c.HQSecret = config.HQSecret
	c.HQStrategy = config.HQStrategy
	c.HQBatchSize = int(config.HQBatchSize)
	c.HQBatchConcurrency = config.HQBatchConcurrency
	c.HQRateLimitingSendBack = config.HQRateLimitSendBack

	// Handover mechanism
	c.UseHandover = config.Handover

	c.UseCommit = !config.NoBatchWriteWAL

	return c, nil
}
