package crawl

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/capture/sitespecific/cloudflarestream"
	"github.com/internetarchive/Zeno/internal/queue"
	"github.com/internetarchive/Zeno/internal/utils"
)

func (c *Crawl) extractAssets(base *url.URL, item *queue.Item, doc *goquery.Document) (assets []*url.URL, err error) {
	var rawAssets []string

	// Execute plugins on the response
	if strings.Contains(base.Host, "cloudflarestream.com") {
		cloudflarestreamURLs, err := cloudflarestream.GetSegments(base, *c.Client)
		if err != nil {
			c.Log.WithFields(c.genLogFields(err, item.URL, nil)).Warn("error getting cloudflarestream segments")
		}

		if len(cloudflarestreamURLs) > 0 {
			assets = append(assets, cloudflarestreamURLs...)
		}
	}

	// Get assets from JSON payloads in data-item values
	doc.Find("[data-item]").Each(func(index int, item *goquery.Selection) {
		dataItem, exists := item.Attr("data-item")
		if exists {
			URLsFromJSON, _ := getURLsFromJSON(dataItem)
			rawAssets = append(rawAssets, URLsFromJSON...)
		}
	})

	// Check all elements style attributes for background-image & also data-preview
	doc.Find("*").Each(func(index int, item *goquery.Selection) {
		style, exists := item.Attr("style")
		if exists {
			re := regexp.MustCompile(`(?:\(['"]?)(.*?)(?:['"]?\))`)
			matches := re.FindAllStringSubmatch(style, -1)

			for match := range matches {
				if len(matches[match]) > 0 {
					rawAssets = append(rawAssets, matches[match][1])
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
			re := regexp.MustCompile(`(?m)url\((.*?)\)`)
			matches := re.FindAllStringSubmatch(item.Text(), -1)

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
					URLsFromJSON, _ := getURLsFromJSON(item.Text())
					rawAssets = append(rawAssets, URLsFromJSON...)
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
						URLsFromJSON, _ := getURLsFromJSON(jsonContent[1][:payloadEndPosition+1])
						rawAssets = append(rawAssets, removeGoogleVideoURLs(URLsFromJSON)...)
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

	// Ensure that excluded hosts aren't in the assets.
	assets = c.excludeHosts(assets)

	// Go over all assets and outlinks and make sure they are absolute links
	assets = utils.MakeAbsolute(base, assets)

	return utils.DedupeURLs(assets), nil
}

func removeGoogleVideoURLs(input []string) (output []string) {
	for _, i := range input {
		if !strings.Contains(i, "googlevideo.com") {
			output = append(output, i)
		}
	}

	return output
}
