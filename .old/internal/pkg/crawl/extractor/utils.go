package extractor

import (
	"net/url"
	"sort"
	"strings"
)

func isContentType(header, targetContentType string) bool {
	// Lowercase the header and target content type for case-insensitive comparison
	header = strings.ToLower(header)
	targetContentType = strings.ToLower(targetContentType)

	return strings.Contains(header, targetContentType)
}

// compareURLs compares two slices of *url.URL
func compareURLs(a, b []*url.URL) bool {
	if len(a) != len(b) {
		return false
	}

	// Create a map to store the count of each URL in slice a
	counts := make(map[string]int)
	for _, url := range a {
		counts[url.String()]++
	}

	// Decrement the count for each URL in slice b
	for _, url := range b {
		counts[url.String()]--
	}

	// Check if any count is non-zero, indicating a mismatch
	for _, count := range counts {
		if count != 0 {
			return false
		}
	}

	return true
}

// sortURLs sorts a slice of *url.URL
func sortURLs(urls []*url.URL) {
	sort.Slice(urls, func(i, j int) bool {
		return urls[i].String() < urls[j].String()
	})
}
