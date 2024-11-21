package ytdlp

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os/exec"
	"strconv"
)

func getJSON(port int) (streamURLs, metaURLs []string, rawJSON string, HTTPHeaders map[string]string, err error) {
	HTTPHeaders = make(map[string]string)

	// Prepare the command
	cmd := exec.Command("yt-dlp", "http://localhost:"+strconv.Itoa(port), "--dump-json", "-f", "bv[protocol=https]+ba[protocol=https]")

	// Buffers to capture stdout and stderr
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Run the command
	err = cmd.Run()
	if err != nil {
		return streamURLs, metaURLs, rawJSON, HTTPHeaders, fmt.Errorf("yt-dlp error: %v\nstderr: %s", err, stderr.String())
	}

	output := stdout.String()

	// Parse the output as a Video object
	var video Video
	err = json.Unmarshal([]byte(output), &video)
	if err != nil {
		return streamURLs, metaURLs, rawJSON, HTTPHeaders, fmt.Errorf("error unmarshaling yt-dlp JSON: %v", err)
	}

	// Get the manifest URL for the best video & audio quality
	// Note: we do not archive live streams
	if !video.IsLive {
		if len(video.RequestedFormats) > 0 {
			HTTPHeaders = video.RequestedFormats[0].HTTPHeaders
			for _, format := range video.RequestedFormats {
				// Choose stream_type=
				// If acodec == "none" and vcodec != "none", it's "video"
				// If acodec != "none" and vcodec == "none", it's "audio"
				// If acodec != "none" and vcodec != "none", we don't specify stream_type
				var streamType string
				if format.Acodec == "none" && format.Vcodec != "none" {
					streamType = "video"
				} else if format.Acodec != "none" && format.Vcodec == "none" {
					streamType = "audio"
				}

				var URL = format.URL + "&video_id=" + video.ID
				if streamType != "" {
					URL += "&stream_type=" + streamType
				}

				streamURLs = append(streamURLs, URL)
			}
		}
	}

	// Get all subtitles (not automatic captions)
	for _, subtitle := range video.Subtitles {
		for _, sub := range subtitle {
			metaURLs = append(metaURLs, sub.URL)
		}
	}

	// Get all thumbnail URLs
	for _, thumbnail := range video.Thumbnails {
		metaURLs = append(metaURLs, thumbnail.URL)
	}

	// Get the storyboards
	for _, format := range video.Formats {
		if format.FormatNote == "storyboard" {
			metaURLs = append(metaURLs, format.URL)
			for _, fragment := range format.Fragments {
				metaURLs = append(metaURLs, fragment.URL)
			}
		}
	}

	return streamURLs, metaURLs, output, HTTPHeaders, nil
}

func FindPath() (string, bool) {
	path, err := exec.LookPath("yt-dlp")
	if err != nil {
		return "", false
	}
	return path, true
}
