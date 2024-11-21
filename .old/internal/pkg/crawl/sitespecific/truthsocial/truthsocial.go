package truthsocial

import (
	"errors"
	"net/url"
	"regexp"
	"strings"
)

var truthSocialPostURLRegex = regexp.MustCompile(`https?://truthsocial\.com/@[A-Za-z0-9_]+/posts/\d+`)

func IsTruthSocialURL(URL string) bool {
	return truthSocialPostURLRegex.MatchString(URL)
}

func extractPostID(URL string) (string, error) {
	splitURL := strings.Split(URL, "/")
	if len(splitURL) < 6 {
		return "", errors.New("invalid URL format")
	}

	return splitURL[5], nil
}

func GenerateAPIURL(URL string) (*url.URL, error) {
	postID, err := extractPostID(URL)
	if err != nil {
		return nil, err
	}

	apiURL, err := url.Parse("https://truthsocial.com/api/v1/statuses/" + postID)
	if err != nil {
		return nil, err
	}

	return apiURL, nil
}

func EmbedURLs() (URLs []*url.URL, err error) {
	URLsString := []string{
		"https://truthsocial.com/api/v1/instance",
		"https://truthsocial.com/api/v2/pepe/instance",
		"https://truthsocial.com/api/v1/pepe/registrations",
		"https://truthsocial.com/packs/js/features/status-c45930b03ed6733263f7.chunk.js",
		"https://truthsocial.com/packs/js/features/ui-41c7fc2c5c89af476253.chunk.js",
		"https://truthsocial.com/packs/js/locale_en-json-6faa20d336d4db2ae5c2.chunk.js",
		"https://truthsocial.com/packs/js/error-f79ccf9f9c62540e8d24.chunk.js",
		"https://truthsocial.com/packs/js/error-7db9c592d5533abc11c4.chunk.js",
		"https://truthsocial.com/packs/js/locale_fr-json-be2806b06f0a4e32cc10.chunk.js",
		"https://truthsocial.com/packs/js/features/status-a9a9466d867b55c49645.chunk.js",
		"https://truthsocial.com/packs/js/features/ui-309139abd01199a782af.chunk.js",
		"https://truthsocial.com/packs/js/features/ui-309139abd01199a782af.chunk.js",
	}

	for _, URL := range URLsString {
		apiURL, err := url.Parse(URL)
		if err != nil {
			return nil, err
		}

		URLs = append(URLs, apiURL)
	}

	return URLs, nil
}
