package youtube

import (
	"net/url"
	"strings"
)

func IsYouTubeWatchPage(URL *url.URL) bool {
	return strings.Contains(URL.Host, "youtube.com") && (strings.Contains(URL.Path, "/watch") || strings.Contains(URL.Path, "/v/"))
}

// func Parse(body io.ReadCloser) (URLs []*url.URL, rawJSON string, HTTPHeaders map[string]string, err error) {
// 	HTTPHeaders = make(map[string]string)

// 	// Call ytdlp on the temporary server
// 	rawURLs, rawJSON, HTTPHeaders, err := ytdlp.GetJSON()
// 	if err != nil {
// 		return nil, rawJSON, HTTPHeaders, err
// 	}

// 	// Parse the URLs
// 	for _, urlString := range rawURLs {
// 		URL, err := url.Parse(urlString)
// 		if err != nil {
// 			return nil, rawJSON, HTTPHeaders, err
// 		}

// 		URLs = append(URLs, URL)
// 	}

// 	return URLs, rawJSON, HTTPHeaders, nil
// }
