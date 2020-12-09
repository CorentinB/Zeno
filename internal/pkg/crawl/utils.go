package crawl

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/CorentinB/Zeno/internal/pkg/frontier"
	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/sirupsen/logrus"
)

var regexOutlinks *regexp.Regexp

func (crawl *Crawl) writeFrontierToDisk() {
	for !crawl.Finished.Get() {
		crawl.Frontier.Save()
		time.Sleep(time.Minute * 1)
	}
}

func extractLinksFromText(source string) (links []url.URL) {
	// Extract links and dedupe them
	rawLinks := utils.DedupeStrings(regexOutlinks.FindAllString(source, -1))

	// Validate links
	for _, link := range rawLinks {
		URL, err := url.Parse(link)
		if err != nil {
			continue
		}
		err = utils.ValidateURL(URL)
		if err != nil {
			continue
		}
		links = append(links, *URL)
	}

	return links
}

func needBrowser(item *frontier.Item) bool {
	res, err := http.Head(item.URL.String())
	if err != nil {
		return true
	}

	// If the Content-Type is text/html, we use a headless browser
	contentType := res.Header.Get("content-type")
	if strings.Contains(contentType, "text/html") {
		return true
	}

	return false
}

func (crawl *Crawl) handleCrawlPause() {
	for {
		if float64(utils.GetFreeDiskSpace(crawl.JobPath).Avail)/float64(GB) <= 20 {
			crawl.Paused.Set(true)
			crawl.Frontier.Paused.Set(true)
		} else {
			crawl.Paused.Set(false)
			crawl.Frontier.Paused.Set(false)
		}

		time.Sleep(time.Second)
	}
}

func (crawl *Crawl) tempFilesCleaner() {
	for {
		files, err := ioutil.ReadDir(path.Join(crawl.JobPath, "temp"))
		if err != nil {
			logrus.Fatal(err)
		}

		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".done") {
				err := os.Remove(path.Join(crawl.JobPath, "temp", file.Name()))
				if err != nil {
					logrus.Fatal(err)
				}
			}
		}

		time.Sleep(time.Second * 1)
	}
}
