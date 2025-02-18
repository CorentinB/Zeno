package preprocessor

import (
	"testing"

	"github.com/internetarchive/Zeno/pkg/models"
)

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		name        string
		rawURL      string
		parentURL   string
		wantErr     bool
		expectedURL string
	}{
		{
			name:        "valid absolute URL",
			rawURL:      "https://example.com/path",
			wantErr:     false,
			expectedURL: "https://example.com/path",
		},
		{
			name:        "valid relative URL with parent",
			rawURL:      "/path",
			parentURL:   "https://example.com",
			wantErr:     false,
			expectedURL: "https://example.com/path",
		},
		{
			name:    "invalid URL",
			rawURL:  "://invalid-url",
			wantErr: true,
		},
		{
			name:        "valid URL without scheme",
			rawURL:      "www.google.com",
			wantErr:     false,
			expectedURL: "http://www.google.com",
		},
		{
			name:    "FTP url",
			rawURL:  "ftp://ftp.example.com",
			wantErr: true,
		},
		{
			name:        "valid URL with path without scheme",
			rawURL:      "www.google.com/dogs",
			wantErr:     false,
			expectedURL: "http://www.google.com/dogs",
		},
		{
			name:        "URL with leading and trailing quotes",
			rawURL:      `"https://example.com/path"`,
			wantErr:     false,
			expectedURL: "https://example.com/path",
		},
		{
			name:        "relative URL with leading and trailing quotes",
			rawURL:      `'/path'`,
			parentURL:   "https://example.com",
			wantErr:     false,
			expectedURL: "https://example.com/path",
		},
		{
			name:        "relative URL without parent",
			rawURL:      "/path",
			wantErr:     true,
			expectedURL: "",
		},
	}

	for _, tt := range tests {
		// TODO: add support for nil value of parentURL
		t.Run(tt.name, func(t *testing.T) {
			url := &models.URL{Raw: tt.rawURL}
			var parentURL *models.URL
			if tt.parentURL != "" {
				parentURL = &models.URL{Raw: tt.parentURL}
				parentURL.Parse()
			}
			err := NormalizeURL(url, parentURL)
			if (err != nil) != tt.wantErr {
				t.Errorf("normalizeURL() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && url.Raw != tt.expectedURL {
				t.Errorf("normalizeURL() got = %v, want %v", url.Raw, tt.expectedURL)
			}
		})
	}
}
