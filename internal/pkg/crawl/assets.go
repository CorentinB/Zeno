package crawl

import (
	"encoding/json"
	"net/url"
	"strings"

	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/PuerkitoBio/goquery"
)

func parseURLFromJSON(value interface{}) (URLs []string) {
	switch JSON := value.(type) {
	case map[string]interface{}:
		for _, v := range JSON {
			switch vChild := v.(type) {
			case string:
				if strings.HasPrefix(vChild, "http") {
					URLs = append(URLs, vChild)
				}
			case map[string]interface{}:
				URLs = append(URLs, parseURLFromJSON(vChild)...)
			}
		}
	default:
		return
	}
	return URLs
}

func (c *Crawl) extractAssets(base *url.URL, doc *goquery.Document) (assets []url.URL, err error) {
	var rawAssets []string

	// Extract assets on the page (images, scripts, videos..)
	if !utils.StringInSlice("img", c.DisabledHTMLTags) {
		doc.Find("img").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
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

	if !utils.StringInSlice("script", c.DisabledHTMLTags) {
		doc.Find("script").Each(func(index int, item *goquery.Selection) {
			link, exists := item.Attr("src")
			if exists {
				rawAssets = append(rawAssets, link)
			}

			scriptType, exists := item.Attr("type")
			if exists {
				if scriptType == "application/json" {
					// Declared an empty interface
					var result map[string]interface{}

					// Unmarshal or Decode the JSON to the interface.
					json.Unmarshal([]byte(item.Text()), &result)
					rawAssets = append(rawAssets, parseURLFromJSON(result)...)
					return
				}
			}

			// Apply regex on the script's HTML to extract potential assets
			outerHTML, err := goquery.OuterHtml(item)
			if err != nil {
				logWarning.Warning(err)
			} else {
				scriptLinks := utils.DedupeStrings(regexOutlinks.FindAllString(outerHTML, -1))
				for _, scriptLink := range scriptLinks {
					if strings.HasPrefix(scriptLink, "http") {
						rawAssets = append(rawAssets, scriptLink)
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
		})
	}

	// Turn strings into url.URL
	assets = utils.StringSliceToURLSlice(rawAssets)

	// Go over all assets and outlinks and make sure they are absolute links
	assets = utils.MakeAbsolute(base, assets)

	return utils.DedupeURLs(assets), nil
}
