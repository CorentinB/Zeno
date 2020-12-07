package utils

import (
	"crypto/sha1"
	"encoding/hex"
	"net/url"
	"strings"
)

// StringSliceToURLSlice takes a slice of string and return a slice of url.URL
func StringSliceToURLSlice(rawURLs []string) (URLs []url.URL) {
	for _, URL := range rawURLs {
		URL, err := url.Parse(URL)
		if err != nil {
			continue
		}

		URLs = append(URLs, *URL)
	}

	return URLs
}

// StringContainsSliceElements if the string contains one of the elements
// of a slice
func StringContainsSliceElements(target string, slice []string) bool {
	for _, elem := range slice {
		if strings.Contains(target, elem) {
			return true
		}
	}
	return false
}

// GetSHA1 take a string and return the SHA1 hash of the string, as a string
func GetSHA1(str string) string {
	hash := sha1.New()
	hash.Write([]byte(str))
	return hex.EncodeToString(hash.Sum(nil))
}

// StringInSlice return true if the string is in the slice
func StringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// DedupeStrings take a slice of string and dedupe it
func DedupeStrings(input []string) []string {
	keys := make(map[string]bool)
	list := []string{}
	for _, entry := range input {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
