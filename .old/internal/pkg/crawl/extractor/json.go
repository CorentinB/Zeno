package extractor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
)

func JSON(resp *http.Response) (URLs []*url.URL, err error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	rawURLs, err := GetURLsFromJSON(body)
	if err != nil {
		return nil, err
	}

	for _, rawURL := range rawURLs {
		URL, err := url.Parse(rawURL)
		if err == nil {
			URLs = append(URLs, URL)
		}
	}

	return URLs, err
}

func GetURLsFromJSON(body []byte) ([]string, error) {
	var data interface{}
	err := json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}

	links := make([]string, 0)
	findURLs(data, &links)

	return links, nil
}

func findURLs(data interface{}, links *[]string) {
	switch v := data.(type) {
	case string:
		if isValidURL(v) {
			*links = append(*links, v)
		}
	case []interface{}:
		for _, element := range v {
			findURLs(element, links)
		}
	case map[string]interface{}:
		for _, value := range v {
			findURLs(value, links)
		}
	}
}

func isValidURL(str string) bool {
	u, err := url.Parse(str)
	return err == nil && u.Scheme != "" && u.Host != ""
}
