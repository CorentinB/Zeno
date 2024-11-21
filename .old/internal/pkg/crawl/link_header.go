package crawl

import (
	"strings"
)

// Represents a Link struct, containing a URL to which it links, and a Rel to define the relation
type Link struct {
	URL string
	Rel string
}

// Parse parses a raw Link header in the form:
//
//	<url1>; rel="what", <url2>; rel="any"; another="yes", <url3>; rel="thing"
//
// returning a slice of Link structs
// Each of these are separated by a `, ` and the in turn by a `; `, with the first always being the url, and the remaining the key-val pairs
// See: https://simon-frey.com/blog/link-header/, https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Link
func Parse(link string) []Link {
	var links []Link

	for _, link := range strings.Split(link, ", ") {
		parts := strings.Split(link, ";")
		if len(parts) < 1 {
			// Malformed input, somehow we didn't get atleast one part
			continue
		}

		url := strings.TrimSpace(strings.Trim(parts[0], "<>"))
		rel := ""

		for _, attrs := range parts[1:] {
			key, value := ParseAttr(attrs)
			if key == "" {
				// Malformed input, somehow the key is nothing
				continue
			}

			if key == "rel" {
				rel = value
				break
			}
		}
		links = append(links, Link{URL: url, Rel: rel})
	}

	return links
}

// Parse a single attribute key value pair and return it
func ParseAttr(attrs string) (key, value string) {
	kv := strings.SplitN(attrs, "=", 2)

	if len(kv) != 2 {
		return "", ""
	}

	key = strings.TrimSpace(kv[0])
	value = strings.TrimSpace(strings.Trim(kv[1], "\""))

	return key, value
}
