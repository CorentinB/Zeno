package extractor

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"strings"
)

var sitemapMarker = []byte("sitemaps.org/schemas/sitemap/")

func XML(resp *http.Response, strict bool) (URLs []*url.URL, sitemap bool, err error) {
	xmlBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, sitemap, err
	}

	if bytes.Contains(xmlBody, sitemapMarker) {
		sitemap = true
	}

	decoder := xml.NewDecoder(bytes.NewReader(xmlBody))
	decoder.Strict = strict

	var tok xml.Token
	for {
		if strict {
			tok, err = decoder.Token()
		} else {
			tok, err = decoder.RawToken()
		}

		if tok == nil && err == io.EOF {
			// normal EOF
			break
		}

		if err != nil {
			// return URLs we got so far when error occurs
			return URLs, sitemap, err
		}

		switch tok := tok.(type) {
		case xml.StartElement:
			for _, attr := range tok.Attr {
				if strings.HasPrefix(attr.Value, "http") {
					parsedURL, err := url.Parse(attr.Value)
					if err == nil {
						URLs = append(URLs, parsedURL)
					}
				}
			}
		case xml.CharData:
			if bytes.HasPrefix(tok, []byte("http")) {
				parsedURL, err := url.Parse(string(tok))
				if err == nil {
					URLs = append(URLs, parsedURL)
				}
			}
		}
	}

	return URLs, sitemap, nil
}
