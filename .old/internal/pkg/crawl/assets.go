package crawl

import (
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/extractor"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/cloudflarestream"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
	"github.com/remeh/sizedwaitgroup"
)

var backgroundImageRegex = regexp.MustCompile(`(?:\(['"]?)(.*?)(?:['"]?\))`)
var urlRegex = regexp.MustCompile(`(?m)url\((.*?)\)`)

func (c *Crawl) captureAsset(item *queue.Item, cookies []*http.Cookie, headers map[string]string) error {
	var resp *http.Response

	// Prepare GET request
	req, err := http.NewRequest("GET", utils.URLToString(item.URL), nil)
	if err != nil {
		return err
	}

	req.Header.Set("Referer", utils.URLToString(item.ParentURL))
	req.Header.Set("User-Agent", c.UserAgent)

	// If headers are passed, apply them to the request
	if headers != nil {
		for key, value := range headers {
			req.Header.Set(key, value)
		}
	}

	// Apply cookies obtained from the original URL captured
	for i := range cookies {
		req.AddCookie(cookies[i])
	}

	resp, err = c.executeGET(item, req, false)
	if err != nil && err.Error() == "URL from redirection has already been seen" {
		return nil
	} else if err != nil {
		return err
	}
	defer resp.Body.Close()

	if extractor.IsM3U8(resp) {
		assets, err := extractor.M3U8(resp)
		if err == nil {
			assets = c.seencheckAssets(assets, item)
			if len(assets) != 0 {
				c.captureAssets(item, assets, cookies, headers)
			}
		} else {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract URLs from M3U8")
		}
	}

	io.Copy(io.Discard, resp.Body)

	return nil
}

func (c *Crawl) captureAssets(item *queue.Item, assets []*url.URL, cookies []*http.Cookie, headers map[string]string) {
	// TODO: implement a counter for the number of assets
	// currently being processed
	// c.Frontier.QueueCount.Incr(int64(len(assets)))
	swg := sizedwaitgroup.New(int(c.MaxConcurrentAssets))
	excluded := false

	for _, asset := range assets {
		// TODO: implement a counter for the number of assets
		// currently being processed
		// c.Frontier.QueueCount.Incr(-1)

		// Just making sure we do not over archive by archiving the original URL
		if utils.URLToString(item.URL) == utils.URLToString(asset) {
			continue
		}

		// If the URL match any excluded string, we ignore it
		for _, excludedString := range c.ExcludedStrings {
			if strings.Contains(utils.URLToString(asset), excludedString) {
				excluded = true
				break
			}
		}

		if excluded {
			excluded = false
			continue
		}

		swg.Add()
		c.URIsPerSecond.Incr(1)

		go func(asset *url.URL, swg *sizedwaitgroup.SizedWaitGroup) {
			defer swg.Done()

			// Create the asset's item
			newAsset, err := queue.NewItem(asset, item.URL, "asset", item.Hop, "", false)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, asset, map[string]interface{}{
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
					"type":      "asset",
				})).Error("error while creating asset item")
				return
			}

			// Capture the asset
			err = c.captureAsset(newAsset, cookies, headers)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, &asset, map[string]interface{}{
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
					"type":      "asset",
				})).Error("error while capturing asset")
				return
			}

			// If we made it to this point, it means that the asset have been crawled successfully,
			// then we can increment the locallyCrawled variable
			atomic.AddUint64(&item.LocallyCrawled, 1)
		}(asset, &swg)
	}

	swg.Wait()
}

func (c *Crawl) seencheckAssets(assets []*url.URL, item *queue.Item) []*url.URL {
	if c.UseSeencheck {
		if c.UseHQ {
			seencheckedURLs, err := c.HQSeencheckURLs(assets)
			// We ignore the error here because we don't want to slow down the crawl
			// if HQ is down or if the request failed. So if we get an error, we just
			// continue with the original list of assets.
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, nil, map[string]interface{}{
					"urls":      assets,
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
				})).Error("error while seenchecking assets via HQ")
			} else {
				assets = seencheckedURLs
			}

			if len(assets) == 0 {
				return []*url.URL{}
			}
		} else {
			seencheckedBatch := []*url.URL{}

			for _, URL := range assets {
				found := c.Seencheck.SeencheckURL(utils.URLToString(URL), "asset")
				if found {
					continue
				}

				seencheckedBatch = append(seencheckedBatch, URL)
			}

			if len(seencheckedBatch) == 0 {
				return []*url.URL{}
			}

			assets = seencheckedBatch
		}
	}

	return assets
}

func (c *Crawl) extractAssets(base *url.URL, item *queue.Item, doc *goquery.Document) (assets []*url.URL, err error) {
	var rawAssets []string
	var URL = utils.URLToString(item.URL)

	// Execute plugins on the response
	if cloudflarestream.IsURL(URL) {
		cloudflarestreamURLs, err := cloudflarestream.GetSegments(base, *c.Client)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Warn("error getting cloudflarestream segments")
		}

		assets = append(assets, cloudflarestreamURLs...)
	}

	// Get assets from JSON payloads in data-item values
	doc.Find("[data-item]").Each(func(index int, item *goquery.Selection) {
		dataItem, exists := item.Attr("data-item")
		if exists {
			URLsFromJSON, err := extractor.GetURLsFromJSON([]byte(dataItem))
			if err != nil {
				c.Log.Error("unable to extract URLs from JSON in data-item attribute", "error", err, "url", URL)
			} else {
				rawAssets = append(rawAssets, URLsFromJSON...)
			}
		}
	})

	// Check all elements style attributes for background-image & also data-preview
	doc.Find("*").Each(func(index int, item *goquery.Selection) {
		style, exists := item.Attr("style")
		if exists {
			matches := backgroundImageRegex.FindAllStringSubmatch(style, -1)

			for match := range matches {
				if len(matches[match]) > 0 {
					matchFound := matches[match][1]
					// Don't extract CSS elements that aren't URLs
					if strings.Contains(matchFound, "%") || strings.HasPrefix(matchFound, "0.") || strings.HasPrefix(matchFound, "--font") || strings.HasPrefix(matchFound, "--size") || strings.HasPrefix(matchFound, "--color") || strings.HasPrefix(matchFound, "--shreddit") || strings.HasPrefix(matchFound, "100vh") {
						continue
					}
					rawAssets = append(rawAssets, matchFound)
				}
			}
		}

		dataPreview, exists := item.Attr("data-preview")
		if exists {
			if strings.HasPrefix(dataPreview, "http") {
				rawAssets = append(rawAssets, dataPreview)
			}
		}
	})

	// Extract assets on the page (images, scripts, videos..)
	if !utils.StringInSlice("img", c.DisabledHTMLTags) {
		doc.Find("img").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			link, exists = item.Attr("data-src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			link, exists = item.Attr("data-lazy-src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			link, exists = item.Attr("data-srcset")
			if exists {
				links := strings.Split(link, ",")
				for _, link := range links {
					rawAssets = append(rawAssets, strings.Split(strings.TrimSpace(link), " ")[0])
				}
			}

			link, exists = item.Attr("srcset")
			if exists {
				links := strings.Split(link, ",")
				for _, link := range links {
					rawAssets = append(rawAssets, strings.Split(strings.TrimSpace(link), " ")[0])
				}
			}
		})
	}

	if !utils.StringInSlice("video", c.DisabledHTMLTags) {
		doc.Find("video").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}
		})
	}

	if !utils.StringInSlice("style", c.DisabledHTMLTags) {
		doc.Find("style").Each(func(index int, item *goquery.Selection) {
			matches := urlRegex.FindAllStringSubmatch(item.Text(), -1)
			for match := range matches {
				matchReplacement := matches[match][1]
				matchReplacement = strings.Replace(matchReplacement, "'", "", -1)
				matchReplacement = strings.Replace(matchReplacement, "\"", "", -1)

				// If the URL already has http (or https), we don't need add anything to it.
				if !strings.Contains(matchReplacement, "http") {
					matchReplacement = strings.Replace(matchReplacement, "//", "http://", -1)
				}

				if strings.HasPrefix(matchReplacement, "#wp-") {
					continue
				}

				rawAssets = append(rawAssets, matchReplacement)
			}
		})
	}

	if !utils.StringInSlice("script", c.DisabledHTMLTags) {
		doc.Find("script").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			scriptType, exists := item.Attr("type")
			if exists {
				if scriptType == "application/json" {
					URLsFromJSON, err := extractor.GetURLsFromJSON([]byte(item.Text()))
					if err != nil {
						// TODO: maybe add back when https://github.com/internetarchive/Zeno/issues/147 is fixed
						// c.Log.Debug("unable to extract URLs from JSON in script tag", "error", err, "url", URL)
					} else {
						rawAssets = append(rawAssets, URLsFromJSON...)
					}
				}
			}

			// Apply regex on the script's HTML to extract potential assets
			outerHTML, err := goquery.OuterHtml(item)
			if err != nil {
				c.Log.Warn("crawl/assets.go:extractAssets():goquery.OuterHtml():", "error", err)
			} else {
				scriptLinks := utils.DedupeStrings(regexOutlinks.FindAllString(outerHTML, -1))
				for _, scriptLink := range scriptLinks {
					if strings.HasPrefix(scriptLink, "http") {
						// Escape URLs when unicode runes are present in the extracted URLs
						scriptLink, err := strconv.Unquote(`"` + scriptLink + `"`)
						if err != nil {
							c.Log.Debug("unable to escape URL from JSON in script tag", "error", err, "url", scriptLink)
							continue
						}
						rawAssets = append(rawAssets, scriptLink)
					}
				}
			}

			// Some <script> embed variable initialisation, we can strip the variable part and just scrape JSON
			if !strings.HasPrefix(item.Text(), "{") {
				jsonContent := strings.SplitAfterN(item.Text(), "=", 2)

				if len(jsonContent) > 1 {
					var (
						openSeagullCount   int
						closedSeagullCount int
						payloadEndPosition int
					)

					// figure out the end of the payload
					for pos, char := range jsonContent[1] {
						if char == '{' {
							openSeagullCount++
						} else if char == '}' {
							closedSeagullCount++
						} else {
							continue
						}

						if openSeagullCount > 0 {
							if openSeagullCount == closedSeagullCount {
								payloadEndPosition = pos
								break
							}
						}
					}

					if len(jsonContent[1]) > payloadEndPosition {
						URLsFromJSON, err := extractor.GetURLsFromJSON([]byte(jsonContent[1][:payloadEndPosition+1]))
						if err != nil {
							// TODO: maybe add back when https://github.com/internetarchive/Zeno/issues/147 is fixed
							// c.Log.Debug("unable to extract URLs from JSON in script tag", "error", err, "url", URL)
						} else {
							rawAssets = append(rawAssets, URLsFromJSON...)
						}
					}
				}
			}
		})
	}

	if !utils.StringInSlice("link", c.DisabledHTMLTags) {
		doc.Find("link").Each(func(index int, item *goquery.Selection) {
			if !c.CaptureAlternatePages {
				relation, exists := item.Attr("rel")
				if exists && relation == "alternate" {
					return
				}
			}

			link, exists := item.Attr("href")
			if exists {
				rawAssets = append(rawAssets, link)
			}
		})
	}

	if !utils.StringInSlice("audio", c.DisabledHTMLTags) {
		doc.Find("audio").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}
		})
	}

	if !utils.StringInSlice("meta", c.DisabledHTMLTags) {
		doc.Find("meta").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("href")
			if exists {
				rawAssets = append(rawAssets, link)
			}
			link, exists = item.Attr("content")
			if exists {
				if strings.Contains(link, "http") {
					rawAssets = append(rawAssets, link)
				}
			}
		})
	}

	if !utils.StringInSlice("source", c.DisabledHTMLTags) {
		doc.Find("source").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			link, exists = item.Attr("srcset")
			if exists {
				links := strings.Split(link, ",")
				for _, link := range links {
					rawAssets = append(rawAssets, strings.Split(strings.TrimSpace(link), " ")[0])
				}
			}

			link, exists = item.Attr("data-srcset")
			if exists {
				links := strings.Split(link, ",")
				for _, link := range links {
					rawAssets = append(rawAssets, strings.Split(strings.TrimSpace(link), " ")[0])
				}
			}
		})
	}

	// Turn strings into url.URL
	assets = append(assets, utils.StringSliceToURLSlice(rawAssets)...)

	// Ensure that no asset that would be excluded is added to the list,
	// remove all fragments, and make sure that all assets are absolute URLs
	assets = c.cleanURLs(base, assets)

	return utils.DedupeURLs(assets), nil
}

func (c *Crawl) cleanURLs(base *url.URL, URLs []*url.URL) (output []*url.URL) {
	// Remove excluded URLs
	for _, URL := range URLs {
		if !c.isExcluded(URL) {
			output = append(output, URL)
		}
	}

	// Make all URLs absolute
	if base != nil {
		output = utils.MakeAbsolute(base, output)
	}

	// Remove fragments
	return utils.RemoveFragments(output)
}
