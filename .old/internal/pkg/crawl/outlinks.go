package crawl

import (
	"net/url"
	"strings"
	"sync"

	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

func (c *Crawl) extractOutlinks(base *url.URL, doc *goquery.Document) (outlinks []*url.URL, err error) {
	var rawOutlinks []string

	// Extract outlinks
	doc.Find("a").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("href")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})

	// Extract iframes as 'outlinks' as they usually can be treated as entirely seperate pages with entirely seperate assets.
	doc.Find("iframe").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("src")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})

	doc.Find("ref").Each(func(index int, item *goquery.Selection) {
		link, exists := item.Attr("target")
		if exists {
			rawOutlinks = append(rawOutlinks, link)
		}
	})

	// Turn strings into url.URL
	outlinks = utils.StringSliceToURLSlice(rawOutlinks)

	// Extract all text on the page and extract the outlinks from it
	textOutlinks := extractLinksFromText(doc.Find("body").RemoveFiltered("script").Text())
	outlinks = append(outlinks, textOutlinks...)

	// Ensure that no outlink that would be excluded is added to the list,
	// remove all fragments, and make sure that all assets are absolute URLs
	outlinks = c.cleanURLs(base, outlinks)

	return utils.DedupeURLs(outlinks), nil
}

func (c *Crawl) queueOutlinks(outlinks []*url.URL, item *queue.Item, wg *sync.WaitGroup) {
	defer wg.Done()

	// Send the outlinks to the pool of workers
	var items = make([]*queue.Item, 0, len(outlinks))
	for _, outlink := range outlinks {
		if c.UseSeencheck {
			if c.Seencheck.SeencheckURL(utils.URLToString(outlink), "seed") {
				continue
			}
		}

		if c.domainsCrawlPass(item.URL, outlink, item.Hop) {
			newItem, err := queue.NewItem(outlink, item.URL, "seed", 0, "", false)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, outlink, nil)).Error("unable to create new item from outlink, discarding")
				continue
			}

			if c.UseHQ {
				c.HQProducerChannel <- newItem
			} else {
				items = append(items, newItem)
			}
		} else if uint64(c.MaxHops) >= item.Hop+1 {
			newItem, err := queue.NewItem(outlink, item.URL, "seed", item.Hop+1, "", false)
			if err != nil {
				c.Log.WithFields(c.genLogFields(err, outlink, nil)).Error("unable to create new item from outlink, discarding")
				continue
			}

			if c.UseHQ {
				c.HQProducerChannel <- newItem
			} else {
				items = append(items, newItem)
			}
		}
	}

	if !c.UseHQ && len(items) > 0 {
		err := c.Queue.BatchEnqueue(items...)
		if err != nil {
			c.Log.Error("unable to enqueue outlinks, discarding", "error", err)
		}
	}
}

func (c *Crawl) domainsCrawlPass(origin, outlink *url.URL, originHop uint64) bool {
	// These are sites that host other sites. When --domains-crawl is used on one of these
	// domains, we want to perform it on the subdomain, not the entire domain.
	var excluded = []string{
		"forumactif.com",
		"canalblog.com",
		"blogspot.com",
		"wordpress.com",
		"tumblr.com",
		"weebly.com",
		"jimdo.com",
		"webnode.com",
		"yola.com",
		"strikingly.com",
		"site123.com",
		"simplesite.com",
		"webstarts.com",
		"webflow.com",
		"weblium.com",
		"webself.net",
		"one.com",
		"free.fr",
		"000webhost.com",
		"hostinger.com",
		"hostgator.com",
		"bluehost.com",
		"godaddy.com",
		"siteground.com",
		"ionos.com",
	}

	if origin == nil || outlink == nil {
		return false
	}

	if !c.DomainsCrawl || originHop != 0 {
		return false
	}

	// Strip out subdomains from both URLs
	originParts := strings.Split(origin.Host, ".")
	if len(originParts) < 2 {
		// ???
		return false
	}

	originDomain := originParts[len(originParts)-2] + "." + originParts[len(originParts)-1]

	outlinkParts := strings.Split(outlink.Host, ".")
	if len(outlinkParts) < 2 {
		// ???
		return false
	}

	outlinkDomain := outlinkParts[len(outlinkParts)-2] + "." + outlinkParts[len(outlinkParts)-1]

	for _, domain := range excluded {
		if originDomain == domain {
			// We want to crawl the subdomain, not the entire domain
			return strings.Contains(outlink.Host, origin.Host)
		}
	}

	return originDomain == outlinkDomain
}
