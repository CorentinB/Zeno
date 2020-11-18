package utils

import (
	"crypto/sha1"
	"encoding/base32"
	"io"
	"log"
	"os"
)

// FileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors
func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// GetSHA1FromFile calculate the SHA1 of a file
func GetSHA1FromFile(filePath string) string {
	hasher := sha1.New()

	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	if _, err := io.Copy(hasher, f); err != nil {
		log.Fatal(err)
	}

	return base32.StdEncoding.EncodeToString(hasher.Sum(nil))
}
