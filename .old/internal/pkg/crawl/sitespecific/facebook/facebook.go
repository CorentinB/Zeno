package facebook

import (
	"fmt"
	"net/url"
	"strings"
)

func IsFacebookPostURL(URL string) bool {
	return strings.Contains(URL, "facebook.com") && strings.Contains(URL, "/posts/")
}

func GenerateEmbedURL(URL string) (*url.URL, error) {
	embedURL, err := url.Parse(fmt.Sprintf("https://www.facebook.com/plugins/post.php?href=%s&show_text=true", url.QueryEscape(URL)))
	if err != nil {
		return nil, err
	}

	return embedURL, nil
}
