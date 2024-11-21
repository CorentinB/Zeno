package extractor

import (
	"net/http"
	"net/url"

	"github.com/grafov/m3u8"
)

func IsM3U8(resp *http.Response) bool {
	return isContentType(resp.Header.Get("Content-Type"), "application/vnd.apple.mpegurl") ||
		isContentType(resp.Header.Get("Content-Type"), "application/x-mpegURL")
}

func M3U8(resp *http.Response) (URLs []*url.URL, err error) {
	playlist, listType, err := m3u8.DecodeFrom(resp.Body, true)
	if err != nil {
		return URLs, err
	}

	var rawURLs []string
	switch listType {
	case m3u8.MEDIA:
		mediapl := playlist.(*m3u8.MediaPlaylist)

		for _, segment := range mediapl.Segments {
			if segment != nil && segment.URI != "" {
				rawURLs = append(rawURLs, segment.URI)
			}
		}
	case m3u8.MASTER:
		masterpl := playlist.(*m3u8.MasterPlaylist)

		for _, variant := range masterpl.Variants {
			if variant != nil {
				if variant.URI != "" {
					rawURLs = append(rawURLs, variant.URI)
				}

				for _, alt := range variant.Alternatives {
					if alt != nil && alt.URI != "" {
						rawURLs = append(rawURLs, alt.URI)
					}
				}
			}
		}
	}

	baseURL := resp.Request.URL

	for _, rawURL := range rawURLs {
		URL, err := url.Parse(rawURL)
		if err == nil {
			if !URL.IsAbs() {
				URL = baseURL.ResolveReference(URL)
			}
			URLs = append(URLs, URL)
		}
	}

	return URLs, nil
}
