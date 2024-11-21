// config.go
package log

import (
	"log/slog"
	"time"
)

// Config defines the configuration for the logging package
type Config struct {
	FileConfig               *LogfileConfig
	StdoutEnabled            bool
	StdoutLevel              slog.Level
	StderrEnabled            bool
	StderrLevel              slog.Level
	RotateLogFile            bool
	RotatePeriod             time.Duration
	ElasticsearchConfig      *ElasticsearchConfig
	RotateElasticSearchIndex bool
}

// LogfileConfig defines the configuration for file logging
type LogfileConfig struct {
	Dir    string
	Prefix string
	Level  slog.Level
}

// ElasticsearchConfig defines the configuration for Elasticsearch logging
type ElasticsearchConfig struct {
	Addresses   []string
	Username    string
	Password    string
	IndexPrefix string
	Level       slog.Level
}

// defaultConfig returns the default configuration
func defaultConfig() *Config {
	return &Config{
		StdoutEnabled: true,
		StdoutLevel:   slog.LevelInfo,
		StderrEnabled: true,
		StderrLevel:   slog.LevelError,
	}
}
