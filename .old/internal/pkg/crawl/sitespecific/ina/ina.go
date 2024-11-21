package ina

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/CorentinB/warc"
	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

var (
	playerVersion     string
	playerVersionLock sync.Mutex
	playerRegex       *regexp.Regexp
)

func init() {
	playerRegex = regexp.MustCompile(`"//ssl\.p\.jwpcdn\.com[^"]+\.js"`)
}

type APIResponse struct {
	ID              string    `json:"id"`
	Title           string    `json:"title"`
	Description     string    `json:"description"`
	DateOfBroadcast time.Time `json:"dateOfBroadcast"`
	Type            string    `json:"type"`
	Duration        int       `json:"duration"`
	Categories      []any     `json:"categories"`
	Credits         []struct {
		Context struct {
			Vocab      string `json:"@vocab"`
			Hydra      string `json:"hydra"`
			Name       string `json:"name"`
			Value      string `json:"value"`
			Attributes string `json:"attributes"`
		} `json:"@context"`
		Type       string `json:"@type"`
		ID         string `json:"@id"`
		Name       string `json:"name"`
		Value      string `json:"value"`
		Attributes []struct {
			Context struct {
				Vocab string `json:"@vocab"`
				Hydra string `json:"hydra"`
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"@context"`
			Type  string `json:"@type"`
			ID    string `json:"@id"`
			Key   string `json:"key"`
			Value string `json:"value"`
		} `json:"attributes"`
	} `json:"credits"`
	Restrictions                 []any  `json:"restrictions"`
	ResourceURL                  string `json:"resourceUrl"`
	ResourceThumbnail            string `json:"resourceThumbnail"`
	RestrictedBroadcastCountries []any  `json:"restrictedBroadcastCountries"`
	EmbedURL                     string `json:"embedUrl"`
	AllowEmbed                   bool   `json:"allowEmbed"`
	Ratio                        string `json:"ratio"`
	CollectionTitle              string `json:"collectionTitle"`
	IsOnline                     bool   `json:"isOnline"`
	AllowAds                     bool   `json:"allowAds"`
	TypeMedia                    string `json:"typeMedia"`
	HideLogo                     bool   `json:"hideLogo"`
	URI                          string `json:"uri"`
	AdvertisingAsset             bool   `json:"advertisingAsset"`
}

func IsURL(req *http.Request) bool {
	return strings.Contains(utils.URLToString(req.URL), "ina.fr")
}

func IsAPIURL(req *http.Request) bool {
	return strings.Contains(utils.URLToString(req.URL), "apipartner.ina.fr") && !strings.Contains(utils.URLToString(req.URL), "playerConfigurations.json")
}

func ExtractPlayerURLs(doc *goquery.Document, c *warc.CustomHTTPClient) []*url.URL {
	var assets []string

	doc.Find("div[data-type=player]").Each(func(i int, s *goquery.Selection) {
		if playerConfigURL, exists := s.Attr("config-url"); exists {
			assets = append(assets, playerConfigURL)
		}

		if assetDetailsURL, exists := s.Attr("asset-details-url"); exists {
			assets = append(assets, assetDetailsURL)
		}

		if posterURL, exists := s.Attr("poster"); exists {
			assets = append(assets, posterURL)
		}
	})

	assets = append(assets, getJWPlayerURLs(c)...)

	return utils.StringSliceToURLSlice(assets)
}

func getJWPlayerURLs(c *warc.CustomHTTPClient) (URLs []string) {
	playerVersionLock.Lock()
	defer playerVersionLock.Unlock()

	if playerVersion == "" {
		resp, err := c.Get("https://player-hub.ina.fr/version")
		if err != nil {
			return URLs
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return URLs
		}

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return URLs
		}

		playerVersion = string(body)

		URLs = append(URLs,
			"https://player-hub.ina.fr/dist/ina-player.min.js?version="+playerVersion,
			"https://player-hub.ina.fr/dist/player-default-skin.min.css?version="+playerVersion,
			"https://player-hub.ina.fr/assets/player/svg/pause.svg",
			"https://player-hub.ina.fr/assets/player/svg/play.svg",
			"https://player-hub.ina.fr/assets/player/svg/backward.svg",
			"https://player-hub.ina.fr/assets/player/svg/forward.svg",
		)

		// Get the JWPlayer JS code
		playerResp, err := c.Get("https://player-hub.ina.fr/js/jwplayer/jwplayer.js?version=" + playerVersion)
		if err != nil {
			return URLs
		}
		defer playerResp.Body.Close()

		if playerResp.StatusCode != http.StatusOK {
			return URLs
		}

		// Find the JWPlayer assets in the JS file
		body, err = io.ReadAll(playerResp.Body)
		if err != nil {
			return URLs
		}

		matches := playerRegex.FindAllString(string(body), -1)

		// Clean up the matches (remove quotes)
		for _, match := range matches {
			URLs = append(URLs, "https:"+match[1:len(match)-1])
		}

		URLs = append(URLs, "https://ssl.p.jwpcdn.com/player/v/"+extractJWPlayerVersion(string(body))+"/jwplayer.core.controls.html5.js")
	}

	return URLs
}

func extractJWPlayerVersion(body string) string {
	lines := strings.Split(body, "\n")
	for _, line := range lines {
		if strings.Contains(line, "JW Player version") {
			return strings.Split(line, "JW Player version ")[1]
		}
	}
	return ""
}

func ExtractMedias(resp *http.Response) ([]*url.URL, error) {
	var assets []string

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var data APIResponse
	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	assets = append(assets, data.ResourceURL, data.ResourceThumbnail, "https://player.ina.fr"+data.EmbedURL, data.URI)

	return utils.StringSliceToURLSlice(assets), nil
}
