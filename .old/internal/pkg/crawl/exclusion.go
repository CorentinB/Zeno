package crawl

import (
	"net/url"
	"strings"

	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

func (c *Crawl) isExcluded(URL *url.URL) bool {
	// If Zeno is ran with --include-host flag,
	// only URLs from the included hosts are crawled
	if !c.isHostIncluded(URL) {
		return false
	}

	// Verify if the URL is excluded by the host
	// (--exclude-host flag)
	if c.isHostExcluded(URL) {
		return true
	}

	// Verify if the URL is excluded by the --exclude-string flag
	for _, excludedString := range c.ExcludedStrings {
		if strings.Contains(utils.URLToString(URL), excludedString) {
			return true
		}
	}

	// If --include-string flag is used, only URLs
	// containing the included strings are crawled
	var includedStringMatch bool
	for _, includedString := range c.IncludedStrings {
		if strings.Contains(utils.URLToString(URL), includedString) {
			includedStringMatch = true
			break
		}
	}

	if len(c.IncludedStrings) > 0 && !includedStringMatch {
		return true
	}

	return false
}

func (c *Crawl) isHostExcluded(URL *url.URL) bool {
	return utils.StringInSlice(URL.Host, c.ExcludedHosts)
}

func (c *Crawl) isHostIncluded(URL *url.URL) bool {
	// If no hosts are included, all hosts are included
	if len(c.IncludedHosts) == 0 {
		return true
	}

	return utils.StringInSlice(URL.Host, c.IncludedHosts)
}
