package crawl

import (
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/utils"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/PuerkitoBio/goquery"
	"github.com/sirupsen/logrus"
)

func (c *Crawl) executeGET(parentItem *frontier.Item, req *http.Request) (resp *http.Response, err error) {
	var newItem *frontier.Item
	var newReq *http.Request
	var URL *url.URL

	// Execute GET request
	if c.ClientProxied == nil || utils.StringContainsSliceElements(req.URL.Host, c.BypassProxy) {
		resp, err = c.Client.Do(req)
		if err != nil {
			return resp, err
		}
	} else {
		resp, err = c.ClientProxied.Do(req)
		if err != nil {
			return resp, err
		}
	}

	// If a redirection is catched, then we execute the redirection
	if isRedirection(resp.StatusCode) {
		if resp.Header.Get("location") == req.URL.String() || parentItem.Redirect >= c.MaxRedirect {
			return resp, nil
		}

		URL, err = url.Parse(resp.Header.Get("location"))
		if err != nil {
			return resp, err
		}

		newItem = frontier.NewItem(URL, parentItem, parentItem.Type, parentItem.Hop)
		newItem.Redirect = parentItem.Redirect + 1

		// Prepare GET request
		newReq, err = http.NewRequest("GET", URL.String(), nil)
		if err != nil {
			return resp, err
		}

		req.Header.Set("User-Agent", c.UserAgent)
		req.Header.Set("Accept-Encoding", "*/*")
		req.Header.Set("Referer", newItem.ParentItem.URL.String())

		resp, err = c.executeGET(newItem, newReq)
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

func (c *Crawl) captureAsset(item *frontier.Item, cookies []*http.Cookie) error {
	var executionStart = time.Now()
	var resp *http.Response

	// If --seencheck is enabled, then we check if the URI is in the
	// seencheck DB before doing anything. If it is in it, we skip the item
	if c.Seencheck {
		hash := strconv.FormatUint(item.Hash, 10)
		found, _ := c.Frontier.Seencheck.IsSeen(hash)
		if found {
			return nil
		}
		c.Frontier.Seencheck.Seen(hash, item.Type)
	}

	// Prepare GET request
	req, err := http.NewRequest("GET", item.URL.String(), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Referer", "https://www.tiktok.com/")

	// Apply cookies obtained from the original URL captured
	for i := range cookies {
		req.AddCookie(cookies[i])
	}

	resp, err = c.executeGET(item, req)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return err
	}
	defer resp.Body.Close()

	c.logCrawlSuccess(executionStart, resp.StatusCode, item)

	return nil
}

// Capture capture the URL and return the outlinks
func (c *Crawl) Capture(item *frontier.Item) {
	var executionStart = time.Now()
	var resp *http.Response

	initReq, err := http.NewRequest("GET", "https://www.tiktok.com/", nil)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}
	if c.ClientProxied == nil {
		resp, err = c.Client.Do(initReq)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
			}).Warning(item.URL.String())
			return
		}
	} else {
		resp, err = c.ClientProxied.Do(initReq)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
			}).Warning(item.URL.String())
			return
		}
	}
	cookies := resp.Cookies()

	// Prepare GET request
	req, err := http.NewRequest("GET", item.URL.String(), nil)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}

	for i := range cookies {
		req.AddCookie(cookies[i])
	}

	if item.Hop > 0 && len(item.ParentItem.URL.String()) > 0 {
		req.Header.Set("Referer", item.ParentItem.URL.String())
	} else {
		req.Header.Set("Referer", "https://www.tiktok.com/")
	}

	resp, err = c.executeGET(item, req)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}
	defer resp.Body.Close()

	c.logCrawlSuccess(executionStart, resp.StatusCode, item)

	// Store the base URL to turn relative links into absolute links later
	base, err := url.Parse(resp.Request.URL.String())
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}

	// Turn the response into a doc that we will scrape
	doc, err := goquery.NewDocumentFromResponse(resp)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}

	// Extract outlinks
	if item.Hop < c.MaxHops {
		outlinks, err := extractOutlinks(base, doc)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
			}).Warning(item.URL.String())
			return
		}
		go c.queueOutlinks(outlinks, item)
	}

	// Extract and capture assets
	assets, err := c.extractAssets(base, doc)
	if err != nil {
		logWarning.WithFields(logrus.Fields{
			"error": err,
		}).Warning(item.URL.String())
		return
	}

	c.Frontier.QueueCount.Incr(int64(len(assets)))
	for _, asset := range assets {
		c.Frontier.QueueCount.Incr(-1)

		// Just making sure we do not over archive
		if item.URL.String() == asset.String() {
			continue
		}

		newAsset := frontier.NewItem(&asset, item, "asset", item.Hop)
		err = c.captureAsset(newAsset, cookies)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error":          err,
				"queued":         c.Frontier.QueueCount.Value(),
				"crawled":        c.Crawled.Value(),
				"rate":           c.URIsPerSecond.Rate(),
				"active_workers": c.ActiveWorkers.Value(),
				"parent_hop":     item.Hop,
				"parent_url":     item.URL.String(),
				"type":           "asset",
			}).Warning(asset.String())
			continue
		}
	}
}
