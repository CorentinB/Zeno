package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Config holds all configuration for our program, parsed from various sources
// The `mapstructure` tags are used to map the fields to the viper configuration
type Config struct {
	LogLevel string `mapstructure:"log-level"`

	Job     string `mapstructure:"job"`
	JobPath string

	// UseSeencheck exists just for convenience of not checking
	// !DisableSeencheck in the rest of the code, to make the code clearer
	DisableSeencheck bool `mapstructure:"disable-seencheck"`
	UseSeencheck     bool

	UserAgent                string   `mapstructure:"user-agent"`
	Cookies                  string   `mapstructure:"cookies"`
	APIPort                  string   `mapstructure:"api-port"`
	PrometheusPrefix         string   `mapstructure:"prometheus-prefix"`
	WARCPrefix               string   `mapstructure:"warc-prefix"`
	WARCOperator             string   `mapstructure:"warc-operator"`
	WARCTempDir              string   `mapstructure:"warc-temp-dir"`
	WARCSize                 int      `mapstructure:"warc-size"`
	WARCOnDisk               bool     `mapstructure:"warc-on-disk"`
	WARCPoolSize             int      `mapstructure:"warc-pool-size"`
	WARCDedupeSize           int      `mapstructure:"warc-dedupe-size"`
	CDXDedupeServer          string   `mapstructure:"warc-cdx-dedupe-server"`
	CDXCookie                string   `mapstructure:"warc-cdx-cookie"`
	HQAddress                string   `mapstructure:"hq-address"`
	HQKey                    string   `mapstructure:"hq-key"`
	HQSecret                 string   `mapstructure:"hq-secret"`
	HQProject                string   `mapstructure:"hq-project"`
	HQStrategy               string   `mapstructure:"hq-strategy"`
	HQBatchSize              int      `mapstructure:"hq-batch-size"`
	HQBatchConcurrency       int      `mapstructure:"hq-batch-concurrency"`
	LogFileOutputDir         string   `mapstructure:"log-file-output-dir"`
	ElasticSearchUsername    string   `mapstructure:"es-user"`
	ElasticSearchPassword    string   `mapstructure:"es-password"`
	ElasticSearchIndexPrefix string   `mapstructure:"es-index-prefix"`
	DisableHTMLTag           []string `mapstructure:"disable-html-tag"`
	ExcludeHosts             []string `mapstructure:"exclude-host"`
	IncludeHosts             []string `mapstructure:"include-host"`
	IncludeString            []string `mapstructure:"include-string"`
	ExcludeString            []string `mapstructure:"exclude-string"`
	ElasticSearchURLs        []string `mapstructure:"es-url"`
	WorkersCount             int      `mapstructure:"workers"`
	MaxConcurrentAssets      int      `mapstructure:"max-concurrent-assets"`
	MaxHops                  int      `mapstructure:"max-hops"`
	MaxRedirect              int      `mapstructure:"max-redirect"`
	MaxRetry                 int      `mapstructure:"max-retry"`
	HTTPTimeout              int      `mapstructure:"http-timeout"`
	CrawlTimeLimit           int      `mapstructure:"crawl-time-limit"`
	CrawlMaxTimeLimit        int      `mapstructure:"crawl-max-time-limit"`
	MinSpaceRequired         int      `mapstructure:"min-space-required"`
	KeepCookies              bool     `mapstructure:"keep-cookies"`
	Headless                 bool     `mapstructure:"headless"`
	JSON                     bool     `mapstructure:"json"`
	Debug                    bool     `mapstructure:"debug"`
	LiveStats                bool     `mapstructure:"live-stats"`
	API                      bool     `mapstructure:"api"`
	Prometheus               bool     `mapstructure:"prometheus"`
	DomainsCrawl             bool     `mapstructure:"domains-crawl"`
	CaptureAlternatePages    bool     `mapstructure:"capture-alternate-pages"`
	DisableLocalDedupe       bool     `mapstructure:"disable-local-dedupe"`
	CertValidation           bool     `mapstructure:"cert-validation"`
	DisableAssetsCapture     bool     `mapstructure:"disable-assets-capture"`
	HQ                       bool     // Special field to check if HQ is enabled depending on the command called
	HQRateLimitSendBack      bool     `mapstructure:"hq-rate-limiting-send-back"`
	NoStdoutLogging          bool     `mapstructure:"no-stdout-log"`
	NoBatchWriteWAL          bool     `mapstructure:"ultrasafe-queue"`
	Handover                 bool     `mapstructure:"handover"`

	// Network
	Proxy              string   `mapstructure:"proxy"`
	DomainsBypassProxy []string `mapstructure:"bypass-proxy"`
	RandomLocalIP      bool     `mapstructure:"random-local-ip"`
	DisableIPv4        bool     `mapstructure:"disable-ipv4"`
	DisableIPv6        bool     `mapstructure:"disable-ipv6"`
	IPv6AnyIP          bool     `mapstructure:"ipv6-anyip"`

	// Dependencies
	NoYTDLP   bool   `mapstructure:"no-ytdlp"`
	YTDLPPath string `mapstructure:"ytdlp-path"`
}

var (
	config *Config
	once   sync.Once
)

// InitConfig initializes the configuration
// Flags -> Env -> Config file -> Consul config
// Latest has precedence over the rest
func InitConfig() error {
	var err error
	once.Do(func() {
		config = &Config{}

		// Check if a config file is provided via flag
		if configFile := viper.GetString("config-file"); configFile != "" {
			viper.SetConfigFile(configFile)
		} else {
			home, err := os.UserHomeDir()
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			viper.AddConfigPath(home)
			viper.SetConfigType("yaml")
			viper.SetConfigName("zeno-config")
		}

		viper.SetEnvPrefix("ZENO")
		replacer := strings.NewReplacer("-", "_", ".", "_")
		viper.SetEnvKeyReplacer(replacer)
		viper.AutomaticEnv()

		if err = viper.ReadInConfig(); err == nil {
			fmt.Println("Using config file:", viper.ConfigFileUsed())
		}

		if viper.GetBool("consul-config") && viper.GetString("consul-address") != "" {
			var consulAddress *url.URL
			consulAddress, err = url.Parse(viper.GetString("consul-address"))
			if err != nil {
				return
			}

			consulPath, consulFile := filepath.Split(viper.GetString("consul-path"))
			viper.AddRemoteProvider("consul", consulAddress.String(), consulPath)
			viper.SetConfigType(filepath.Ext(consulFile))
			viper.SetConfigName(strings.TrimSuffix(consulFile, filepath.Ext(consulFile)))

			if err = viper.ReadInConfig(); err == nil {
				fmt.Println("Using config file:", viper.ConfigFileUsed())
			}
		}

		// This function is used to bring logic to the flags when needed (e.g. live-stats)
		handleFlagsEdgeCases()

		// This function is used to handle flags aliases (e.g. hops -> max-hops)
		handleFlagsAliases()

		// Unmarshal the config into the Config struct
		err = viper.Unmarshal(config)
	})
	return err
}

// BindFlags binds the flags to the viper configuration
// This is needed because viper doesn't support same flag name accross multiple commands
// Details here: https://github.com/spf13/viper/issues/375#issuecomment-794668149
func BindFlags(flagSet *pflag.FlagSet) {
	flagSet.VisitAll(func(flag *pflag.Flag) {
		viper.BindPFlag(flag.Name, flag)
	})
}

// Get returns the config struct
func Get() *Config {
	return config
}

func GenerateCrawlConfig() error {
	// If the job name isn't specified, we generate a random name
	if config.Job == "" {
		if config.HQProject != "" {
			config.Job = config.HQProject
		} else {
			UUID, err := uuid.NewUUID()
			if err != nil {
				slog.Error("cmd/utils.go:InitCrawlWithCMD():uuid.NewUUID()", "error", err)
				return err
			}

			config.Job = UUID.String()
		}
	}

	config.JobPath = path.Join("jobs", config.Job)
	config.UseSeencheck = !config.DisableSeencheck

	// Defaults --max-crawl-time-limit to 10% more than --crawl-time-limit
	if config.CrawlMaxTimeLimit == 0 && config.CrawlTimeLimit != 0 {
		config.CrawlMaxTimeLimit = config.CrawlTimeLimit + (config.CrawlTimeLimit / 10)
	}

	// We exclude some hosts by default
	config.ExcludeHosts = utils.DedupeStrings(append(config.ExcludeHosts, "archive.org", "archive-it.org"))

	if config.WARCTempDir == "" {
		config.WARCTempDir = path.Join(config.JobPath, "temp")
	}

	if config.UserAgent == "" {
		version := utils.GetVersion()

		// If Version is a commit hash, we only take the first 7 characters
		if len(version.Version) >= 40 {
			version.Version = version.Version[:7]
		}

		config.UserAgent = "Mozilla/5.0 (compatible; archive.org_bot +http://archive.org/details/archive.org_bot) Zeno/" + version.Version + " warc/" + version.WarcVersion
		slog.Info("User-Agent set to", "user-agent", config.UserAgent)
	}

	if config.RandomLocalIP {
		slog.Warn("Random local IP is enabled")
	}

	if config.DisableIPv4 && config.DisableIPv6 {
		slog.Error("Both IPv4 and IPv6 are disabled, at least one of them must be enabled.")
		os.Exit(1)
	} else if config.DisableIPv4 {
		slog.Info("IPv4 is disabled")
	} else if config.DisableIPv6 {
		slog.Info("IPv6 is disabled")
	}

	return nil
}

func handleFlagsEdgeCases() {
	if viper.GetBool("live-stats") {
		// If live-stats is true, set no-stdout-log to true
		viper.Set("no-stdout-log", true)
	}

	if viper.GetBool("prometheus") {
		// If prometheus is true, set no-stdout-log to true
		viper.Set("api", true)
	}
}

func handleFlagsAliases() {
	// For each flag we want to alias, we check if the original flag is at default and if the alias is not
	// If so, we set the original flag to the value of the alias
	if viper.GetUint("hops") != 0 && viper.GetUint("max-hops") == 0 {
		viper.Set("max-hops", viper.GetUint("hops"))
	}

	if viper.GetInt("ca") != 8 && viper.GetInt("max-concurrent-assets") == 8 {
		viper.Set("max-concurrent-assets", viper.GetInt("ca"))
	}

	if viper.GetInt("msr") != 20 && viper.GetInt("min-space-required") == 20 {
		viper.Set("min-space-required", viper.GetInt("msr"))
	}
}
