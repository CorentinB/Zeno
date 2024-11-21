package youtube

import (
	"os"
	"testing"

	"github.com/internetarchive/Zeno/internal/pkg/crawl/dependencies/ytdlp"
)

func TestParse(t *testing.T) {
	// Make io.ReadCloser from the youtube_test.html file
	f, err := os.Open("youtube_test.html")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Parse the video
	streamURLs, metaURLs, rawJSON, _, err := ytdlp.Parse(f)
	if err != nil {
		_, found := ytdlp.FindPath()
		if !found {
			// TODO: install yt-dlp when running our tests in CI?
			t.Skipf("yt-dlp not installed. skipping test due to missing executable.")
			return
		}
		t.Fatal(err)
	}

	// Check the raw JSON
	if rawJSON == "" {
		t.Fatal("Expected non-empty raw JSON")
	}

	// Check the number of URLs
	expected := 174
	if len(streamURLs)+len(metaURLs) != expected {
		t.Fatalf("Expected %d URLs, got %d", expected, len(streamURLs)+len(metaURLs))
	}
}
