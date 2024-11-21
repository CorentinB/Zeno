package hq

import (
	"log/slog"
	"time"

	"github.com/internetarchive/Zeno/internal/pkg/config"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
	"github.com/internetarchive/gocrawlhq"
)

// This function connects to HQ's websocket and listen for messages.
// It also sends and "identify" message to the HQ to let it know that
// Zeno is connected. This "identify" message is sent every second and
// contains the crawler's stats and details.
func Websocket() {
	var identifyTicker = time.NewTicker(time.Second)

	defer func() {
		identifyTicker.Stop()
	}()

	for {
		err := globalHQ.client.Identify(&gocrawlhq.IdentifyMessage{
			Project:   config.Get().HQProject,
			Job:       config.Get().Job,
			IP:        utils.GetOutboundIP().String(),
			Hostname:  utils.GetHostname(),
			GoVersion: utils.GetVersion().GoVersion,
		})
		if err != nil {
			slog.Error("error sending identify payload to Crawl HQ, trying to reconnect", "err", err.Error())

			err = globalHQ.client.InitWebsocketConn()
			if err != nil {
				slog.Error("error initializing websocket connection to crawl HQ", "err", err.Error())
			}
		}

		<-identifyTicker.C
	}
}
