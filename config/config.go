package config

import "github.com/urfave/cli/v2"

type Flags struct {
	UserAgent string
	Job       string
	Workers   int
	MaxHops   uint
	Headless  bool
	Seencheck bool
	JSON      bool
	Debug     bool

	DisabledHTMLTags      cli.StringSlice
	ExcludedHosts         cli.StringSlice
	DomainsCrawl          bool
	CaptureAlternatePages bool
	MaxRedirect           int
	MaxRetry              int

	Proxy       string
	BypassProxy cli.StringSlice

	API              bool
	APIPort          string
	Prometheus       bool
	PrometheusPrefix string

	WARC         bool
	WARCPrefix   string
	WARCOperator string

	Kafka              bool
	KafkaFeedTopic     string
	KafkaOutlinksTopic string
	KafkaConsumerGroup string
	KafkaBrokers       cli.StringSlice
}

type Application struct {
	Flags Flags
}

var App *Application

func init() {
	App = &Application{}
}
