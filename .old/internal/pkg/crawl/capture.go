package crawl

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/dependencies/ytdlp"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/extractor"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/cloudflarestream"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/facebook"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/ina"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/libsyn"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/reddit"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/telegram"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/tiktok"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/truthsocial"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/vk"
	"github.com/internetarchive/Zeno/internal/pkg/crawl/sitespecific/youtube"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

func (c *Crawl) executeGET(item *queue.Item, req *http.Request, isRedirection bool) (resp *http.Response, err error) {
	var (
		executionStart = time.Now()
		newItem        *queue.Item
		newReq         *http.Request
		URL            *url.URL
	)

	// defer func() {
	// 	if c.PrometheusMetrics != nil {
	// 		c.PrometheusMetrics.DownloadedURI.Inc()
	// 	}

	// 	c.URIsPerSecond.Incr(1)

	// 	if item.Type == "seed" {
	// 		c.CrawledSeeds.Incr(1)
	// 	} else if item.Type == "asset" {
	// 		c.CrawledAssets.Incr(1)
	// 	}
	// }()

	// // Check if the crawl is paused
	// for c.Paused.Get() {
	// 	time.Sleep(time.Second)
	// }

	// Retry on 429 error
	for retry := uint8(0); retry < c.MaxRetry; retry++ {
		// Execute GET request
		if c.ClientProxied == nil || utils.StringContainsSliceElements(req.URL.Host, c.BypassProxy) {
			resp, err = c.Client.Do(req)
		} else {
			resp, err = c.ClientProxied.Do(req)
		}

		// This is unused unless there is an error or a 429.
		sleepTime := time.Second * time.Duration(retry*2)

		if err != nil {
			if retry+1 >= c.MaxRetry {
				return nil, err
			}
			if strings.Contains(err.Error(), "unsupported protocol scheme") || strings.Contains(err.Error(), "no such host") {
				return nil, err
			}

			c.Log.WithFields(c.genLogFields(err, req.URL, nil)).Error("error while executing GET request, retrying", "retries", retry)

			time.Sleep(sleepTime)

			continue
		}

		if resp.StatusCode == 429 {
			// This is the last retry attempt
			if retry+1 >= c.MaxRetry {
				// Don't close the body, just return the response
				return resp, nil
			}

			c.Log.WithFields(c.genLogFields(err, req.URL, map[string]interface{}{
				"sleepTime":  sleepTime.String(),
				"retryCount": retry,
				"statusCode": resp.StatusCode,
			})).Info("we are being rate limited")

			// This ensures we aren't leaving the warc dialer hanging.
			// Do note, 429s are filtered out by WARC writer regardless.
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()

			// If --hq-rate-limiting-send-back is enabled, we send the URL back to HQ
			if c.UseHQ && c.HQRateLimitingSendBack {
				return nil, errors.New("URL is being rate limited, sending back to HQ")
			}
			c.Log.WithFields(c.genLogFields(err, req.URL, map[string]interface{}{
				"sleepTime":  sleepTime.String(),
				"retryCount": retry,
				"statusCode": resp.StatusCode,
			})).Warn("URL is being rate limited")

			time.Sleep(sleepTime)
			continue
		}
		c.logCrawlSuccess(executionStart, resp.StatusCode, item)
		break
	}

	// If a redirection is caught, then we execute the redirection
	if isStatusCodeRedirect(resp.StatusCode) {
		if resp.Header.Get("location") == utils.URLToString(req.URL) || item.Redirect >= uint64(c.MaxRedirect) {
			return resp, nil
		}
		defer resp.Body.Close()

		// Needed for WARC writing
		// IMPORTANT! This will write redirects to WARC!
		io.Copy(io.Discard, resp.Body)

		URL, err = url.Parse(resp.Header.Get("location"))
		if err != nil {
			return resp, err
		}

		// Make URL absolute if they aren't.
		// Some redirects don't return full URLs, but rather, relative URLs. We would still like to follow these redirects.
		if !URL.IsAbs() {
			URL = req.URL.ResolveReference(URL)
		}

		// Seencheck the URL
		if c.UseSeencheck {
			if c.UseHQ {
				isNewURL, err := c.HQSeencheckURL(URL)
				if err != nil {
					return resp, err
				}

				if !isNewURL {
					return nil, errors.New("URL from redirection has already been seen")
				}
			} else {
				found := c.Seencheck.SeencheckURL(utils.URLToString(URL), "seed")
				if found {
					return nil, errors.New("URL from redirection has already been seen")
				}
			}
		}

		newItem, err = queue.NewItem(URL, item.URL, item.Type, item.Hop, item.ID, false)
		if err != nil {
			return nil, err
		}

		newItem.Redirect = item.Redirect + 1

		// Prepare GET request
		newReq, err = http.NewRequest("GET", utils.URLToString(URL), nil)
		if err != nil {
			return nil, err
		}

		// Set new request headers on the new request
		newReq.Header.Set("User-Agent", c.UserAgent)
		newReq.Header.Set("Referer", utils.URLToString(newItem.ParentURL))

		return c.executeGET(newItem, newReq, true)
	}

	return resp, nil
}

// Capture capture the URL and return the outlinks
func (c *Crawl) Capture(item *queue.Item) error {
	var (
		resp      *http.Response
		waitGroup sync.WaitGroup
		assets    []*url.URL
	)

	defer func(i *queue.Item) {
		waitGroup.Wait()

		if c.UseHQ && i.ID != "" {
			c.HQFinishedChannel <- i
		}
	}(item)

	// Prepare GET request
	req, err := http.NewRequest("GET", utils.URLToString(item.URL), nil)
	if err != nil {
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while preparing GET request")
		return err
	}

	if item.Hop > 0 && item.ParentURL != nil {
		req.Header.Set("Referer", utils.URLToString(item.ParentURL))
	}

	req.Header.Set("User-Agent", c.UserAgent)

	// Execute site-specific code on the request, before sending it
	if truthsocial.IsTruthSocialURL(utils.URLToString(item.URL)) {
		// Get the API URL from the URL
		APIURL, err := truthsocial.GenerateAPIURL(utils.URLToString(item.URL))
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating API URL")
		} else {
			if APIURL == nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating API URL")
			} else {
				// Then we create an item
				APIItem, err := queue.NewItem(APIURL, item.URL, item.Type, item.Hop, item.ID, false)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating TruthSocial API item")
				} else {
					err = c.Capture(APIItem)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing TruthSocial API URL")
					}
				}
			}

			// Grab few embeds that are needed for the playback
			embedURLs, err := truthsocial.EmbedURLs()
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while getting TruthSocial embed URLs")
			} else {
				for _, embedURL := range embedURLs {
					// Create the embed item
					embedItem, err := queue.NewItem(embedURL, item.URL, item.Type, item.Hop, item.ID, false)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating TruthSocial embed item")
					} else {
						err = c.Capture(embedItem)
						if err != nil {
							c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing TruthSocial embed URL")
						}
					}
				}
			}
		}
	} else if facebook.IsFacebookPostURL(utils.URLToString(item.URL)) {
		// Generate the embed URL
		embedURL, err := facebook.GenerateEmbedURL(utils.URLToString(item.URL))
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating Facebook embed URL")
		} else {
			if embedURL == nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating Facebook embed URL")
			} else {
				// Create the embed item
				embedItem, err := queue.NewItem(embedURL, item.URL, item.Type, item.Hop, item.ID, false)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating Facebook embed item")
				} else {
					err = c.Capture(embedItem)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing Facebook embed URL")
					}
				}
			}
		}
	} else if libsyn.IsLibsynURL(utils.URLToString(item.URL)) {
		// Generate the highwinds URL
		highwindsURL, err := libsyn.GenerateHighwindsURL(utils.URLToString(item.URL))
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating libsyn URL")
		} else {
			if highwindsURL == nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while generating libsyn URL")
			} else {
				highwindsItem, err := queue.NewItem(highwindsURL, item.URL, item.Type, item.Hop, item.ID, false)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating libsyn highwinds item")
				} else {
					err = c.Capture(highwindsItem)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing libsyn highwinds URL")
					}
				}
			}
		}
	} else if tiktok.IsTikTokURL(utils.URLToString(item.URL)) {
		tiktok.AddHeaders(req)
	} else if telegram.IsTelegramURL(utils.URLToString(item.URL)) && !telegram.IsTelegramEmbedURL(utils.URLToString(item.URL)) {
		// If the URL is a Telegram URL, we make an embed URL out of it
		telegram.TransformURL(item.URL)

		// Then we create an item
		embedItem, err := queue.NewItem(item.URL, item.URL, item.Type, item.Hop, item.ID, false)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating Telegram embed item")
		} else {
			// And capture it
			err = c.Capture(embedItem)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing Telegram embed URL")
			}
		}
	} else if vk.IsVKURL(utils.URLToString(item.URL)) {
		vk.AddHeaders(req)
	} else if reddit.IsURL(utils.URLToString(item.URL)) {
		reddit.AddCookies(req)
	}

	// Execute request
	resp, err = c.executeGET(item, req, false)
	if err != nil && err.Error() == "URL from redirection has already been seen" {
		return err
	} else if err != nil && err.Error() == "URL is being rate limited, sending back to HQ" {
		newItem, err := queue.NewItem(item.URL, item.ParentURL, item.Type, item.Hop, "", true)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating new item")
			return err
		}

		c.HQProducerChannel <- newItem
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("URL is being rate limited, sending back to HQ")
		return err
	} else if err != nil {
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while executing GET request")
		return err
	}
	defer resp.Body.Close()

	// If it was a YouTube watch page, we potentially want to run it through the YouTube extractor
	// TODO: support other watch page URLs
	if !c.NoYTDLP && youtube.IsYouTubeWatchPage(item.URL) {
		streamURLs, metaURLs, rawJSON, HTTPHeaders, err := ytdlp.Parse(resp.Body)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while parsing YouTube watch page")
			return err
		}
		resp.Body.Close()

		// Capture the 2 stream URLs for the video
		var streamErrs []error
		var streamWg sync.WaitGroup

		for _, streamURL := range streamURLs {
			streamWg.Add(1)
			go func(streamURL *url.URL) {
				defer streamWg.Done()
				resp, err := c.executeGET(item, &http.Request{
					Method: "GET",
					URL:    streamURL,
				}, false)
				if err != nil {
					streamErrs = append(streamErrs, fmt.Errorf("error executing GET request for %s: %w", streamURL, err))
					return
				}
				defer resp.Body.Close()

				if resp.StatusCode != 200 {
					streamErrs = append(streamErrs, fmt.Errorf("invalid status code for %s: %s", streamURL, resp.Status))
					return
				}

				_, err = io.Copy(io.Discard, resp.Body)
				if err != nil {
					streamErrs = append(streamErrs, fmt.Errorf("error reading response body for %s: %w", streamURL, err))
				}
			}(streamURL)
		}

		streamWg.Wait()

		if len(streamErrs) > 0 {
			for _, err := range streamErrs {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while capturing stream URL")
			}
			return fmt.Errorf("errors occurred while capturing stream URLs: %v", streamErrs)
		}

		// Write the metadata record for the video
		if rawJSON != "" {
			c.Client.WriteRecord(utils.URLToString(item.URL), "metadata", "application/json; metadata-type=ia-video; generator=yt-dlp", rawJSON)
		}

		if len(metaURLs) > 0 {
			c.captureAssets(item, metaURLs, resp.Cookies(), HTTPHeaders)
		}

		return nil
	} else if reddit.IsPostAPI(req) {
		permalinks, rawAssets, err := reddit.ExtractPost(resp)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract post from Reddit")
		}

		// Queue the permalinks
		waitGroup.Add(1)
		go c.queueOutlinks(utils.StringSliceToURLSlice(permalinks), item, &waitGroup)

		// Capture the assets (if any)
		if len(rawAssets) != 0 {
			assets = utils.StringSliceToURLSlice(rawAssets)

			assets = c.seencheckAssets(assets, item)
			if len(assets) != 0 {
				c.captureAssets(item, assets, resp.Cookies(), nil)
			}
		}

		return nil
	} else if ina.IsAPIURL(req) {
		rawAssets, err := ina.ExtractMedias(resp)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract medias from INA")
		}

		if len(rawAssets) != 0 {
			assets = c.seencheckAssets(rawAssets, item)

			if len(assets) != 0 {
				for _, asset := range rawAssets {
					playerItem, err := queue.NewItem(asset, item.URL, "seed", 0, "", false)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to create new item from asset")
					} else {
						c.Capture(playerItem)
					}
				}
			}
		}
	}

	// Scrape potential URLs from Link HTTP header
	var (
		links      = Parse(resp.Header.Get("link"))
		discovered []string
	)

	for _, link := range links {
		discovered = append(discovered, link.URL)
	}

	waitGroup.Add(1)
	go c.queueOutlinks(utils.MakeAbsolute(item.URL, utils.StringSliceToURLSlice(discovered)), item, &waitGroup)

	// Store the base URL to turn relative links into absolute links later
	base, err := url.Parse(utils.URLToString(resp.Request.URL))
	if err != nil {
		c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while parsing base URL")
		return err
	}

	// If the response is an XML document, we want to scrape it for links
	var outlinks []*url.URL
	if strings.Contains(resp.Header.Get("Content-Type"), "xml") {
		if extractor.IsS3(resp) {
			URLsFromS3, err := extractor.S3(resp)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while extracting URLs from S3")
			}

			outlinks = append(outlinks, URLsFromS3...)
		} else {
			URLsFromXML, isSitemap, err := extractor.XML(resp, false)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract URLs from XML")
			}
			if len(URLsFromXML) > 0 {
				if isSitemap {
					outlinks = append(outlinks, URLsFromXML...)
				} else {
					assets = append(assets, URLsFromXML...)
				}
			}
		}
	} else if strings.Contains(resp.Header.Get("Content-Type"), "json") {
		assets, err = extractor.JSON(resp)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract URLs from JSON")
		}
	} else if extractor.IsM3U8(resp) {
		assets, err = extractor.M3U8(resp)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to extract URLs from M3U8")
		}
	} else if !strings.Contains(resp.Header.Get("Content-Type"), "text/") || (c.DisableAssetsCapture && !c.DomainsCrawl && (uint64(c.MaxHops) <= item.Hop)) {
		// If the response isn't a text/*, we do not scrape it.
		// We also aren't going to scrape if assets and outlinks are turned off.
		_, err := io.Copy(io.Discard, resp.Body)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while reading response body")
		}

		return err
	} else {
		// Turn the response into a doc that we will scrape for outlinks and assets.
		doc, err := goquery.NewDocumentFromReader(resp.Body)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while creating goquery document")
			return err
		}

		// Execute site-specific code on the document
		if cloudflarestream.IsURL(utils.URLToString(item.URL)) {
			// Look for JS files necessary for the playback of the video
			cfstreamURLs, err := cloudflarestream.GetJSFiles(doc, base, *c.Client)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while getting JS files from cloudflarestream")
				return err
			}

			// Seencheck the URLs we captured, we ignore the returned value here
			// because we already archived the URLs, we just want them to be added
			// to the seencheck table.
			if c.UseSeencheck {
				if c.UseHQ {
					_, err := c.HQSeencheckURLs(utils.StringSliceToURLSlice(cfstreamURLs))
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, map[string]interface{}{
							"urls": cfstreamURLs,
						})).Error("error while seenchecking assets via HQ")
					}
				} else {
					for _, cfstreamURL := range cfstreamURLs {
						c.Seencheck.SeencheckURL(cfstreamURL, "asset")
					}
				}
			}
			// Log the archived URLs
			for _, cfstreamURL := range cfstreamURLs {
				c.Log.WithFields(c.genLogFields(err, cfstreamURL, map[string]interface{}{
					"parentHop": item.Hop,
					"parentUrl": utils.URLToString(item.URL),
					"type":      "asset",
				})).Info("URL archived")
			}
		} else if ina.IsURL(req) {
			playerURLs := ina.ExtractPlayerURLs(doc, c.Client)

			for _, playerURL := range playerURLs {
				playerItem, err := queue.NewItem(playerURL, item.URL, "seed", 0, "", false)
				if err != nil {
					c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("unable to create new item from player URL")
				} else {
					c.Capture(playerItem)
				}
			}
		}

		// Websites can use a <base> tag to specify a base for relative URLs in every other tags.
		// This checks for the "base" tag and resets the "base" URL variable with the new base URL specified
		// https://developer.mozilla.org/en-US/docs/Web/HTML/Element/base
		if !utils.StringInSlice("base", c.DisabledHTMLTags) {
			oldBase := base

			doc.Find("base").Each(func(index int, goitem *goquery.Selection) {
				// If a new base got scraped, stop looking for one
				if oldBase != base {
					return
				}

				// Attempt to get a new base value from the base HTML tag
				link, exists := goitem.Attr("href")
				if exists {
					baseTagValue, err := url.Parse(link)
					if err != nil {
						c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while parsing base tag value")
					} else {
						base = baseTagValue
					}
				}
			})
		}

		// Extract outlinks
		outlinks, err = c.extractOutlinks(base, doc)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while extracting outlinks")
			return err
		}

		if !c.DisableAssetsCapture {
			assets, err = c.extractAssets(base, item, doc)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Error("error while extracting assets")
				return err
			}
		}
	}

	waitGroup.Add(1)
	go c.queueOutlinks(outlinks, item, &waitGroup)

	if !c.DisableAssetsCapture && len(assets) != 0 {
		assets = c.seencheckAssets(assets, item)
		if len(assets) != 0 {
			c.captureAssets(item, assets, resp.Cookies(), nil)
		}
	}

	return nil
}
