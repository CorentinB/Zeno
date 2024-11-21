// destination.go
package log

import (
	"log/slog"
)

// Destination interface
type Destination interface {
	Enabled() bool
	Level() slog.Level
	Write(entry *logEntry)
	Close()
}

func initDestinations() []Destination {
	var destinations []Destination

	if config.StdoutEnabled {
		destinations = append(destinations, &StdoutDestination{
			level: config.StdoutLevel,
		})
	}

	if config.StderrEnabled {
		destinations = append(destinations, &StderrDestination{
			level: config.StderrLevel,
		})
	}

	if config.FileConfig != nil {
		fileDest := NewFileDestination(config.FileConfig)
		destinations = append(destinations, fileDest)
	}

	if config.ElasticsearchConfig != nil {
		esDest := NewElasticsearchDestination(config.ElasticsearchConfig)
		destinations = append(destinations, esDest)
	}

	return destinations
}
