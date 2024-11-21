package extractor

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
)

func TestXML(t *testing.T) {
	tests := []struct {
		name               string
		xmlBody            string
		wantURLsLax        []*url.URL
		wantURLsStric      []*url.URL
		wantURLsCountLax   int
		wantURLsCountStric int
		wantErrLax         bool
		wantErrStrict      bool
		sitemap            bool
	}{
		{
			name: "Valid XML with URLs",
			xmlBody: `
				<root>
					<item>http://example.com</item>
					<nested>
						<url>https://example.org</url>
					</nested>
					<noturl>just some text</noturl>
				</root>`,
			wantURLsLax: []*url.URL{
				{Scheme: "http", Host: "example.com"},
				{Scheme: "https", Host: "example.org"},
			},
			wantURLsStric: []*url.URL{
				{Scheme: "http", Host: "example.com"},
				{Scheme: "https", Host: "example.org"},
			},
			sitemap: false,
		},
		{
			name: "unbalanced XML with URLs",
			xmlBody: `
				<unbalance>
					<url>http://example.com</url>
				</unbalance></unbalance></unbalance>
					<outsideurl>https://unclosed.example.com</outsideurl>`,
			wantURLsStric: []*url.URL{
				{Scheme: "http", Host: "example.com"},
			},
			wantURLsLax: []*url.URL{
				{Scheme: "http", Host: "example.com"},
				{Scheme: "https", Host: "unclosed.example.com"},
			},
			wantErrStrict: true,
			wantErrLax:    false,
			sitemap:       false,
		},
		{
			name:          "Empty XML",
			xmlBody:       `<root></root>`,
			wantURLsStric: nil,
			wantURLsLax:   nil,
			sitemap:       false,
		},
		{
			name:          "alien XML",
			xmlBody:       `<h4 73><?/>/<AS "='AS "ASD@'SD>,as;g^&R$W#Sf)(U><l;rpkv ]])`,
			wantURLsStric: nil,
			wantURLsLax:   nil,
			wantErrStrict: true,
			wantErrLax:    true,
			sitemap:       false,
		},
		{
			name: "XML with invalid URL",
			xmlBody: `
				<root>
					<item>http://example.com</item>
					<item>not a valid url</item>
				</root>`,
			wantURLsStric: []*url.URL{
				{Scheme: "http", Host: "example.com"},
			},
			wantURLsLax: []*url.URL{
				{Scheme: "http", Host: "example.com"},
			},
			wantErrStrict: false,
			wantErrLax:    false,
			sitemap:       false,
		},
		{
			name:               "Huge sitemap",
			xmlBody:            loadTestFile(t, "xml_test_sitemap.xml"),
			wantURLsCountStric: 100002,
			wantURLsCountLax:   100002,
			wantErrStrict:      false,
			wantErrLax:         false,
			sitemap:            true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testMode := func(strict bool, wantErr bool, wantURLs []*url.URL, wantURLsCount int) {
				resp := &http.Response{
					Body: io.NopCloser(bytes.NewBufferString(tt.xmlBody)),
				}
				gotURLs, sitemap, err := XML(resp, strict)
				if (err != nil) != wantErr {
					t.Errorf("XML() strict = %v, error = %v, wantErr %v", strict, err, wantErr)
					return
				}
				if wantURLsCount != 0 && len(gotURLs) != wantURLsCount {
					t.Errorf("XML() strict = %v, gotURLs count = %v, want %v", strict, len(gotURLs), wantURLsCount)
				}
				if wantURLs != nil && !compareURLs(gotURLs, wantURLs) {
					t.Errorf("XML() strict = %v, gotURLs = %v, want %v", strict, gotURLs, wantURLs)
				}
				if tt.sitemap != sitemap {
					t.Errorf("XML() strict = %v, sitemap = %v, want %v", strict, sitemap, tt.sitemap)
				}
			}

			// Strict mode
			testMode(true, tt.wantErrStrict, tt.wantURLsStric, tt.wantURLsCountStric)

			// Lax mode
			testMode(false, tt.wantErrLax, tt.wantURLsLax, tt.wantURLsCountLax)
		})
	}
}

func loadTestFile(t *testing.T, path string) string {
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("openFile() error = %v", err)
	}

	b, err := io.ReadAll(f)
	if err != nil {
		t.Fatalf("readFile() error = %v", err)
	}

	return string(b)
}

func TestXMLBodySyntaxEOFErrorStrict(t *testing.T) {
	wantErr := xml.SyntaxError{Line: 3, Msg: "unexpected EOF"}
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(
			`<unclosed>
			<closed>
			</closed> <!-- Syntax EOF here -->`)),
	}
	_, _, err := XML(resp, true)
	if err == nil {
		t.Errorf("XML() error = %v, wantErr %v", err, wantErr)
		return
	}
	if err.Error() != wantErr.Error() {
		t.Errorf("XML() error = %v, wantErr %v", err, wantErr)
	}
}

func TestXMLBodySyntaxEOFErrorLax(t *testing.T) {
	resp := &http.Response{
		Body: io.NopCloser(strings.NewReader(`<unclosed>
			<closed>
			</closed> <!-- ignore Syntax EOF here -->`)),
	}
	_, _, err := XML(resp, false)
	if err != nil {
		t.Errorf("XML() error = %v, wantErr nil", err)
	}
}
