package ytdlp

import (
	"io"
	"net"
	"net/http"
	"strings"
)

func serveBody(body io.ReadCloser) (port int, stopChan chan struct{}, err error) {
	stopChan = make(chan struct{})
	portChan := make(chan int)

	bodyBytes, err := io.ReadAll(body)
	if err != nil {
		return 0, nil, err
	}

	// Start the server
	go func() {
		// Serve the body on the random port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		portChan <- listener.Addr().(*net.TCPAddr).Port

		go func() {
			<-stopChan
			listener.Close()
		}()

		// Create a handler that will serve the body on /
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write(bodyBytes)
		})

		if err := http.Serve(listener, handler); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			return
		}
	}()

	return <-portChan, stopChan, nil
}
