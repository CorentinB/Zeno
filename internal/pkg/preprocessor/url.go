package preprocessor

import (
	"net/url"

	"github.com/ada-url/goada"
	"github.com/internetarchive/Zeno/pkg/models"
)

// Normalize the URL by removing fragments, attempting to add URL scheme if missing,
// and converting relative URLs into absolute URLs. An error is returned if the URL
// cannot be normalized.
func normalizeURL(URL *models.URL, parentURL *models.URL) (err error) {
	var adaParse *goada.Url

	if parentURL == nil {
		parsedURL, err := url.Parse(URL.Raw)
		if err != nil {
			return err
		}

		if parsedURL.Scheme == "" {
			parsedURL.Scheme = "http"
		}

		adaParse, err = goada.New(models.URLToString(parsedURL))
		if err != nil {
			return err
		}
	} else {
		adaParse, err = goada.NewWithBase(URL.Raw, parentURL.String())
		if err != nil {
			return err
		}
	}

	adaParse.SetHash("")
	if scheme := adaParse.Protocol(); scheme != "http:" && scheme != "https:" {
		return ErrUnsupportedScheme
	}
	URL.Raw = adaParse.Href()

	return URL.Parse()
}
