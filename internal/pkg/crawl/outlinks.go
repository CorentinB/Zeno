package crawl

import (
	"net/url"
	"strings"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/PuerkitoBio/goquery"
)

func extractOutlinks(base *url.URL, doc *goquery.Document) (outlinks []url.URL, err error) {
	var rawOutlinks []string

	// Extract outlinks
	doc.Find("a").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("href")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})
	// Turn strings into url.URL
	outlinks = utils.StringSliceToURLSlice(rawOutlinks)

	// Extract all text on the page and extract the outlinks from it
	textOutlinks := extractLinksFromText(doc.Find("body").RemoveFiltered("script").Text())
	for _, link := range textOutlinks {
		outlinks = append(outlinks, link)
	}

	// Go over all outlinks and make sure they are absolute links
	outlinks = utils.MakeAbsolute(base, outlinks)

	return utils.DedupeURLs(outlinks), nil
}

func (c *Crawl) queueOutlinks(outlinks []url.URL, item *frontier.Item) {
	// Send the outlinks to the pool of workers
	for _, outlink := range outlinks {
		outlink := outlink

		// If the host of the outlink is in the host exclusion list, we ignore it
		if utils.StringInSlice(outlink.Host, c.ExcludedHosts) {
			continue
		}

		if c.DomainsCrawl && strings.Contains(item.Host, outlink.Host) && item.Hop == 0 {
			newItem := frontier.NewItem(&outlink, item, "seed", 0)
			if c.UseKafka && len(c.KafkaOutlinksTopic) > 0 {
				c.KafkaProducerChannel <- newItem
			} else {
				c.Frontier.PushChan <- newItem
			}
		} else {
			newItem := frontier.NewItem(&outlink, item, "seed", item.Hop+1)
			if c.UseKafka && len(c.KafkaOutlinksTopic) > 0 {
				c.KafkaProducerChannel <- newItem
			} else {
				c.Frontier.PushChan <- newItem
			}
		}
	}
}
