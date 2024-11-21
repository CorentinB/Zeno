package extractor

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

var validS3Servers = []string{
	"AmazonS3",
	"WasabiS3",
	"UploadServer", // Google Cloud Storage
	"Windows-Azure-Blob",
	"AliyunOSS", // Alibaba Object Storage Service
}

// S3ListBucketResult represents the XML structure of an S3 bucket listing
type S3ListBucketResult struct {
	XMLName               xml.Name       `xml:"ListBucketResult"`
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Marker                string         `xml:"Marker"`
	Contents              []S3Object     `xml:"Contents"`
	CommonPrefixes        []CommonPrefix `xml:"CommonPrefixes"`
	IsTruncated           bool           `xml:"IsTruncated"`
	NextContinuationToken string         `xml:"NextContinuationToken"`
}

type S3Object struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	Size         int64  `xml:"Size"`
}

type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}

// IsS3 checks if the response is from an S3 server
func IsS3(resp *http.Response) bool {
	return utils.StringContainsSliceElements(resp.Header.Get("Server"), validS3Servers)
}

// S3 takes an initial response and returns URLs of either files or prefixes at the current level,
// plus continuation URL if more results exist
func S3(resp *http.Response) ([]*url.URL, error) {
	result, err := S3ProcessResponse(resp)
	if err != nil {
		return nil, err
	}

	// Extract base URL from the response URL
	reqURL := resp.Request.URL
	requestQuery := reqURL.Query()
	baseURL := fmt.Sprintf("https://%s", reqURL.Host)
	parsedBase, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("invalid base URL: %v", err)
	}

	var urls []string

	// Ensure we can add marker
	// ListObjects
	if requestQuery.Get("list-type") != "2" && len(result.Contents) > 0 {
		// If we can, iterate through S3 using the marker field
		nextURL := *reqURL
		q := nextURL.Query()
		q.Set("marker", result.Contents[len(result.Contents)-1].Key)
		nextURL.RawQuery = q.Encode()
		urls = append(urls, nextURL.String())
	}

	// If we are using list-type 2/ListObjectsV2
	if len(result.CommonPrefixes) > 0 {
		for _, prefix := range result.CommonPrefixes {
			nextURL := *reqURL
			q := nextURL.Query()
			q.Set("prefix", prefix.Prefix)
			nextURL.RawQuery = q.Encode()
			urls = append(urls, nextURL.String())
		}
	} else {
		// Otherwise return file URLs
		for _, obj := range result.Contents {
			if obj.Size > 0 {
				fileURL := *parsedBase
				fileURL.Path += "/" + obj.Key
				urls = append(urls, fileURL.String())
			}
		}
	}

	// If there's a continuation token, add the continuation URL
	if result.IsTruncated && result.NextContinuationToken != "" {
		nextURL := *reqURL
		q := nextURL.Query()
		q.Set("continuation-token", result.NextContinuationToken)
		nextURL.RawQuery = q.Encode()
		urls = append(urls, nextURL.String())
	}

	return utils.StringSliceToURLSlice(urls), nil
}

// S3ProcessResponse parses an HTTP response into an S3ListBucketResult
func S3ProcessResponse(resp *http.Response) (*S3ListBucketResult, error) {
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %v", err)
	}
	defer resp.Body.Close()

	var result S3ListBucketResult
	if err := xml.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("error parsing XML: %v", err)
	}

	return &result, nil
}
