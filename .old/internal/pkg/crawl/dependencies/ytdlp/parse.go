package ytdlp

import (
	"io"
	"net/url"
)

func Parse(body io.ReadCloser) (streamURLs, metaURLs []*url.URL, rawJSON string, HTTPHeaders map[string]string, err error) {
	// Create a temporary server to serve the body and call ytdlp on it
	port, stopChan, err := serveBody(body)
	if err != nil {
		return streamURLs, metaURLs, rawJSON, HTTPHeaders, err
	}
	defer close(stopChan)

	// Call ytdlp on the temporary server
	rawStreamURLs, rawMetaURLs, rawJSON, HTTPHeaders, err := getJSON(port)
	if err != nil {
		return streamURLs, metaURLs, rawJSON, HTTPHeaders, err
	}

	// Range over rawStreamURLs and rawMetaURLs to parse them as url.URL in videoURLs and metaURLs
	for _, urlString := range rawStreamURLs {
		URL, err := url.Parse(urlString)
		if err != nil {
			return streamURLs, metaURLs, rawJSON, HTTPHeaders, err
		}

		streamURLs = append(streamURLs, URL)
	}

	for _, urlString := range rawMetaURLs {
		URL, err := url.Parse(urlString)
		if err != nil {
			return streamURLs, metaURLs, rawJSON, HTTPHeaders, err
		}

		metaURLs = append(metaURLs, URL)
	}

	return streamURLs, metaURLs, rawJSON, HTTPHeaders, nil
}
