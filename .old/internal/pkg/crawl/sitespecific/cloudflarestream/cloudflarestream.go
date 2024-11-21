package cloudflarestream

import (
	"encoding/xml"
	"errors"
	"io"
	"io/ioutil"
	"math"
	"net/url"
	"strconv"
	"strings"

	"github.com/CorentinB/warc"
	"github.com/PuerkitoBio/goquery"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

type MPD struct {
	XMLName                   xml.Name `xml:"MPD"`
	Text                      string   `xml:",chardata"`
	Xmlns                     string   `xml:"xmlns,attr"`
	Profiles                  string   `xml:"profiles,attr"`
	Type                      string   `xml:"type,attr"`
	MediaPresentationDuration string   `xml:"mediaPresentationDuration,attr"`
	MinBufferTime             string   `xml:"minBufferTime,attr"`
	Period                    struct {
		Text          string `xml:",chardata"`
		ID            string `xml:"id,attr"`
		AdaptationSet []struct {
			Text             string `xml:",chardata"`
			ID               string `xml:"id,attr"`
			MimeType         string `xml:"mimeType,attr"`
			SegmentAlignment string `xml:"segmentAlignment,attr"`
			Lang             string `xml:"lang,attr"`
			Representation   []struct {
				Text                      string `xml:",chardata"`
				ID                        string `xml:"id,attr"`
				AudioSamplingRate         string `xml:"audioSamplingRate,attr"`
				Bandwidth                 string `xml:"bandwidth,attr"`
				Codecs                    string `xml:"codecs,attr"`
				FrameRate                 string `xml:"frameRate,attr"`
				Height                    string `xml:"height,attr"`
				Width                     string `xml:"width,attr"`
				AudioChannelConfiguration struct {
					Text        string `xml:",chardata"`
					SchemeIdUri string `xml:"schemeIdUri,attr"`
					Value       string `xml:"value,attr"`
				} `xml:"AudioChannelConfiguration"`
				SegmentTemplate struct {
					Text           string `xml:",chardata"`
					Duration       string `xml:"duration,attr"`
					Initialization string `xml:"initialization,attr"`
					Media          string `xml:"media,attr"`
					StartNumber    string `xml:"startNumber,attr"`
					Timescale      string `xml:"timescale,attr"`
				} `xml:"SegmentTemplate"`
			} `xml:"Representation"`
		} `xml:"AdaptationSet"`
	} `xml:"Period"`
}

func IsURL(URL string) bool {
	return strings.Contains(URL, "cloudflarestream.com")
}

func GetJSFiles(doc *goquery.Document, watchPageURL *url.URL, httpClient warc.CustomHTTPClient) (archivedURLs []string, err error) {
	var latestJSURL string

	// Look for the <script> tag that contains the URL to the latest JS file
	doc.Find("script").Each(func(i int, s *goquery.Selection) {
		// Look for the src attribute
		src, exists := s.Attr("src")
		if exists {
			// If the src attribute contains the string "latest.js", then we found the right script tag
			if strings.Contains(src, "latest.js") {
				latestJSURL = src
				return
			}
		}
	})

	// Now get that file and parse it to find a string starting with iframe and ending with .html
	resp, err := httpClient.Get(latestJSURL)
	if err != nil {
		return archivedURLs, err
	}
	defer resp.Body.Close()

	archivedURLs = append(archivedURLs, latestJSURL)

	// Check that the status code is 200
	if resp.StatusCode == 301 {
		// If the status code is 301, then the URL is a redirect, so we need to follow it
		location, err := resp.Location()
		if err != nil {
			return archivedURLs, err
		}

		resp.Body.Close()

		// Get the new URL
		resp, err = httpClient.Get(utils.URLToString(location))
		if err != nil {
			return archivedURLs, err
		}
		defer resp.Body.Close()

		archivedURLs = append(archivedURLs, utils.URLToString(location))
	}

	if resp.StatusCode != 200 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: status code is not 200, got " + strconv.Itoa(resp.StatusCode))
	}

	// Read the body of the response
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return archivedURLs, err
	}

	// Find location of ".concat("iframe"
	filenameIndex := strings.Index(string(body), ".concat(\"iframe")
	if filenameIndex == -1 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: could not find iframe")
	}

	// Find location of ".html" after ".concat("iframe"
	extensionIndex := strings.Index(string(body[filenameIndex:]), ".html")
	if extensionIndex == -1 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: could not find iframe")
	}

	// Get the URL
	iframeFilename := string(body[filenameIndex+9 : filenameIndex+extensionIndex+5])

	// Get the base URL
	baseURL, err := url.Parse(latestJSURL)
	if err != nil {
		return archivedURLs, err
	}

	// Get the video ID from the watchPageURL, it's the string between the first slash after the host and the second slash
	videoID := strings.Replace(strings.Replace(utils.URLToString(watchPageURL), "/watch", "", 1), "https://"+watchPageURL.Host+"/", "", 1)

	// Build the iframe URL
	iframeURLString := baseURL.Scheme + "://" + baseURL.Host + "/embed/" + iframeFilename + "?videoId=" + videoID

	// Parse the URL
	iframeURL, err := url.Parse(iframeURLString)
	if err != nil {
		return archivedURLs, err
	}

	// Now we have the URL to the iframe HTML, using that page
	// we will look for the iframe-player JS file
	var iframePlayerURL string

	iframeURLResp, err := httpClient.Get(utils.URLToString(iframeURL))
	if err != nil {
		return archivedURLs, err
	}
	defer iframeURLResp.Body.Close()

	archivedURLs = append(archivedURLs, utils.URLToString(iframeURL))

	// Check that the status code is 200
	if iframeURLResp.StatusCode == 301 {
		// If the status code is 301, then the URL is a redirect, so we need to follow it
		location, err := iframeURLResp.Location()
		if err != nil {
			return archivedURLs, err
		}

		iframeURLResp.Body.Close()

		// Get the new URL
		iframeURLResp, err = httpClient.Get(utils.URLToString(location))
		if err != nil {
			return archivedURLs, err
		}
		defer iframeURLResp.Body.Close()

		archivedURLs = append(archivedURLs, utils.URLToString(location))
	}

	if iframeURLResp.StatusCode != 200 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: status code is not 200, got " + strconv.Itoa(iframeURLResp.StatusCode))
	}

	// Turn the body into a document we can parse
	iframeDoc, err := goquery.NewDocumentFromReader(iframeURLResp.Body)
	if err != nil {
		return archivedURLs, err
	}

	// Look for the <script> tag that contains the URL to the latest JS file
	iframeDoc.Find("script").Each(func(i int, s *goquery.Selection) {
		// Look for the src attribute that contains "iframe-player"
		src, exists := s.Attr("src")
		if exists {
			// If the src attribute contains the string "iframe-player", then we found the right script tag
			if strings.Contains(src, "iframe-player") {
				iframePlayerURL = src
				return
			}
		}
	})

	if iframePlayerURL == "" {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: could not find iframe-player")
	}

	// Make the URL absolute
	iframePlayerURL = baseURL.Scheme + "://" + baseURL.Host + "/embed/" + iframePlayerURL

	// Fetch that JS file and parse it to find the potential chunk.js files
	iframePlayerResp, err := httpClient.Get(iframePlayerURL)
	if err != nil {
		return archivedURLs, err
	}
	defer iframePlayerResp.Body.Close()

	archivedURLs = append(archivedURLs, iframePlayerURL)

	// Check that the status code is 200
	if iframePlayerResp.StatusCode == 301 {
		// If the status code is 301, then the URL is a redirect, so we need to follow it
		location, err := iframePlayerResp.Location()
		if err != nil {
			return archivedURLs, err
		}

		iframePlayerResp.Body.Close()

		// Get the new URL
		iframePlayerResp, err = httpClient.Get(utils.URLToString(location))
		if err != nil {
			return archivedURLs, err
		}
		defer iframePlayerResp.Body.Close()

		archivedURLs = append(archivedURLs, utils.URLToString(location))
	}

	if iframePlayerResp.StatusCode != 200 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: status code is not 200, got " + strconv.Itoa(iframePlayerResp.StatusCode) + " for " + iframePlayerURL)
	}

	// Read the body of the response
	iframePlayerBody, err := ioutil.ReadAll(iframePlayerResp.Body)
	if err != nil {
		return archivedURLs, err
	}

	// We are now looking for an dictionary that has numbers as keys and strings as values
	// Remove everything after "chunk.js" (the dict is before it)
	chunkJSIndex := strings.Index(string(iframePlayerBody), "chunk.js")
	if chunkJSIndex == -1 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: could not find chunk.js")
	}

	chunkJSObject := string(iframePlayerBody[:chunkJSIndex])
	chunkJSObject = strings.ReplaceAll(chunkJSObject, "}[e]+\".", "")

	// Find the last occurence of "{"
	openingBracketIndex := strings.LastIndex(chunkJSObject, "{")
	if openingBracketIndex == -1 {
		return archivedURLs, errors.New("cloudflarestream.GetJSFiles: could not find opening bracket")
	}

	// Remove everything before the last "{"
	chunkJSObject = chunkJSObject[openingBracketIndex+1:]

	// Modify the string to make it cuttable
	chunkJSObject = strings.ReplaceAll(chunkJSObject, ":\"", ".")
	chunkJSObject = strings.ReplaceAll(chunkJSObject, "\",", ".chunk.js ")
	chunkJSObject = strings.ReplaceAll(chunkJSObject, "\"", ".chunk.js")

	// Split the string into a slice
	chunkJSObjectSlice := strings.Split(chunkJSObject, " ")

	// Capture the chunk.js files
	for _, chunkJSObjectSliceItem := range chunkJSObjectSlice {
		URL := baseURL.Scheme + "://" + baseURL.Host + "/embed/" + chunkJSObjectSliceItem

		resp, err := httpClient.Get(URL)
		if err != nil {
			return archivedURLs, err
		}

		archivedURLs = append(archivedURLs, URL)

		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}

	// Capture additional JS files that are needed
	var URLs = []string{
		baseURL.Scheme + "://" + watchPageURL.Host + "/" + videoID + "/metadata/playerEnhancementInfo.json",
		baseURL.Scheme + "://" + watchPageURL.Host + "/" + videoID + "/lifecycle",
		baseURL.Scheme + "://" + watchPageURL.Host + "/" + videoID + "/thumbnails/thumbnail.jpg?height=720",
		baseURL.Scheme + "://" + watchPageURL.Host + "/favicon.ico",
	}

	for _, URL := range URLs {
		resp, err := httpClient.Get(URL)
		if err != nil {
			return archivedURLs, err
		}

		archivedURLs = append(archivedURLs, URL)

		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}

	return archivedURLs, nil
}

func GetSegments(URL *url.URL, httpClient warc.CustomHTTPClient) (URLs []*url.URL, err error) {
	var (
		mpd        MPD
		mpdURL     string
		xmlDecoder xml.Decoder
	)

	// Replace /watch with /manifest/video.mpd if the URL ends with /watch, else, raise an error
	if len(utils.URLToString(URL)) < 6 {
		return nil, errors.New("cloudflaresteam.GetSegments: URL too short")
	} else {
		if strings.HasSuffix(utils.URLToString(URL), "/watch") {
			mpdURL = strings.Replace(utils.URLToString(URL), "/watch", "/manifest/video.mpd?parentOrigin="+URL.Scheme+"://"+URL.Host, 1)
		} else {
			return nil, errors.New("cloudflaresteam.GetSegments: URL does not end with /watch")
		}
	}

	// Get the MPD file from the new URL
	resp, err := httpClient.Get(mpdURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Verify that the content-type is application/dash+xml and that the status code is 200
	if resp.StatusCode != 200 {
		return nil, errors.New("cloudflaresteam.GetSegments: status code is not 200")
	}

	if resp.Header.Get("Content-Type") != "application/dash+xml" {
		return nil, errors.New("cloudflaresteam.GetSegments: content-type is not application/dash+xml")
	}

	// Unmarshal the MPD file into a struct with the xml package
	xmlDecoder = *xml.NewDecoder(resp.Body)
	err = xmlDecoder.Decode(&mpd)
	if err != nil {
		return nil, err
	}

	// Convert mpd.MediaPresentationDuration which is in the iso 8601 format to a time.Duration
	duration := parseDuration(mpd.MediaPresentationDuration)

	// For each AdaptationSet tag in the MPD file, parse the duration and timescale from the SegmentTemplate tag
	for _, adaptationSet := range mpd.Period.AdaptationSet {
		for _, representation := range adaptationSet.Representation {
			var (
				segmentTemplate = representation.SegmentTemplate
				timescale       float64
				segmentDuration float64
			)

			// Get the init.mp4 from the initialization attribute and strip the ../../ from the beginning
			initURL := strings.Replace(segmentTemplate.Initialization, "../../", "", 1)
			URLs = append(URLs, &url.URL{
				Scheme: URL.Scheme,
				Host:   URL.Host,
				Path:   initURL,
			})

			timescale, err = strconv.ParseFloat(segmentTemplate.Timescale, 64)
			if err != nil {
				return nil, err
			}

			segmentDuration, err = strconv.ParseFloat(segmentTemplate.Duration, 64)
			if err != nil {
				return nil, err
			}

			// Calculate the number of segments in the video
			nbSegments := math.Ceil(float64(duration) * timescale / segmentDuration)

			// For each segment, create a new URL and append it to the return URLs
			for i := 0; i < int(nbSegments); i++ {
				var (
					segmentURL = segmentTemplate.Media
					segmentNum = strconv.Itoa(i + 1)
				)

				segmentURL = strings.Replace(segmentURL, "$Number$", segmentNum, 1)

				// Strip out ../../ from the URL
				segmentURL = strings.Replace(segmentURL, "../../", "", -1)

				URLs = append(URLs, &url.URL{
					Scheme: URL.Scheme,
					Host:   URL.Host,
					Path:   segmentURL,
				})
			}
		}
	}

	return URLs, err
}

func parseDuration(duration string) int {
	var (
		days, hours, minutes int
		seconds              float64
	)

	duration = strings.TrimPrefix(duration, "P")

	for {
		var idx = strings.IndexAny(duration, "DTHM")

		if idx == -1 {
			break
		}

		var num, _ = strconv.Atoi(duration[:idx])

		switch duration[idx] {
		case 'D':
			days = num
		case 'H':
			hours = num
		case 'M':
			minutes = num
		}

		duration = duration[idx+1:]
	}

	if strings.HasSuffix(duration, "S") {
		duration = strings.TrimSuffix(duration, "S")
		seconds, _ = strconv.ParseFloat(duration, 64)
		seconds = math.Ceil(seconds)
	}

	return days*24*60*60 + hours*60*60 + minutes*60 + int(seconds)
}
