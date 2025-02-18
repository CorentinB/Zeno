package preprocessor

import (
	"net/url"
	"strings"

	"github.com/internetarchive/Zeno/pkg/models"
)

// NormalizeURL removes fragments, ensures a valid scheme, resolves relative paths
// against a parent, and returns an error if the URL cannot be normalized.
func NormalizeURL(URL *models.URL, parentURL *models.URL) (err error) {
	// Clean the URL by removing leading and trailing quotes
	URL.Raw = strings.Trim(URL.Raw, `"'`)

	parsedURL, err := url.Parse(URL.Raw)
	if err != nil {
		return err
	}

	if parentURL != nil && !parsedURL.IsAbs() {
		// Determine the base with the following logic:
		// - always with the <base> tag found in the HTML document, if it exists (TBI)
		// - if the URL starts with a slash, use the parent URL's scheme and host
		// - if the URL does not start with a slash, use the parent URL's scheme, host, and path
		baseURL := parentURL.GetParsed()

		if strings.HasPrefix(parsedURL.Path, "/") {
			// Just scheme + host
			b, errParse := url.Parse(baseURL.Scheme + "://" + baseURL.Host)
			if errParse != nil {
				return errParse
			}

			// Resolve relative URL against this new base
			parsedURL = b.ResolveReference(parsedURL)
		} else {
			// Use full parent URL
			b, errParse := url.Parse(baseURL.String())
			if errParse != nil {
				return errParse
			}

			parsedURL = b.ResolveReference(parsedURL)
		}
	} else {
		// If the URL is absolute or there is no parent
		if parsedURL.Scheme == "" {
			parsedURL.Scheme = "http"
		}

		// This is needed to repopulate the URL fields
		// after modifying the scheme (e.g. the Host would be empty)
		parsedURL, err = url.Parse(parsedURL.String())
		if err != nil {
			return err
		}
	}

	// Remove fragment
	parsedURL.Fragment = ""

	// Check for supported schemes
	if parsedURL.Scheme != "http" && parsedURL.Scheme != "https" {
		return ErrUnsupportedScheme
	}

	// Check for localhost and 127.0.0.1
	host := parsedURL.Hostname()
	if host == "localhost" || host == "127.0.0.1" {
		return ErrUnsupportedHost
	}

	// Check for TLD
	if !strings.Contains(host, ".") {
		return ErrUnsupportedHost
	}

	// Final URL string
	URL.Raw = parsedURL.String()

	return URL.Parse()
}
