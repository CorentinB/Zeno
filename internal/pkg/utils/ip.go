package utils

import (
	"log/slog"
	"net"
	"os"
)

// Note: GetOutboundIP does not establish any connection and the
// destination does not need to exist for this function to work.
func GetOutboundIP() net.IP {
	var (
		conn net.Conn
		err  error
	)

	for {
		conn, err = net.Dial("udp", "24.24.24.24:24200")
		if err != nil {
			slog.Error("error getting outbound IP, retrying", "err", err)
			continue
		}
		defer conn.Close()
		break
	}

	return conn.LocalAddr().(*net.UDPAddr).IP
}

func GetHostname() string {
	hostname, err := os.Hostname()
	if err != nil {
		slog.Error("error getting hostname", "err", err)
	}

	return hostname
}
