package crawl

import (
	"errors"
	"net/http"
	"net/http/httputil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/CorentinB/warc"
	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// errNoBody is a sentinel error value used by failureToReadBody so we
// can detect that the lack of body was intentional.
var errNoBody = errors.New("sentinel error value")

// dumpResponseToFile is like httputil.DumpResponse but dumps the response directly
// to a file and return its path
func (c *Crawl) dumpResponseToFile(resp *http.Response) (string, error) {
	var err error

	// Generate a file on disk with a unique name
	UUID := uuid.NewV4()
	filePath := filepath.Join(c.JobPath, "temp", UUID.String()+".temp")
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Write the response to the file directly
	err = resp.Write(file)
	if err != nil {
		return "", err
	}

	return filePath, nil
}

func (c *Crawl) initWARCWriter() {
	var rotatorSettings = warc.NewRotatorSettings()
	var err error

	os.MkdirAll(path.Join(c.JobPath, "temp"), os.ModePerm)

	rotatorSettings.OutputDirectory = path.Join(c.JobPath, "warcs")
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = c.WARCPrefix
	rotatorSettings.WarcinfoContent.Set("software", "Zeno")
	if len(c.WARCOperator) > 0 {
		rotatorSettings.WarcinfoContent.Set("operator", c.WARCOperator)
	}

	c.WARCWriter, c.WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Fatal("Error when initialize WARC writer")
	}
}

func (c *Crawl) writeWARC(resp *http.Response) (string, error) {
	var batch = warc.NewRecordBatch()
	var requestDump []byte
	var responseDump []byte
	var responsePath string
	var err error

	// Initialize the response record
	var responseRecord = warc.NewRecord()
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("WARC-Target-URI", resp.Request.URL.String())
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	// If the Content-Length is unknown or if it is higher than 2048 bytes, then
	// we process the response directly on disk to not risk maxing-out the RAM.
	// Else, we use the httputil.DumpResponse function to dump the response.
	if resp.ContentLength == -1 || resp.ContentLength > 2048 {
		responsePath, err = c.dumpResponseToFile(resp)
		if err != nil {
			err := os.Remove(responsePath)
			if err != nil {
				logWarning.WithFields(logrus.Fields{
					"path":  responsePath,
					"error": err,
				}).Warning("Error deleting temporary file")
			}
			return responsePath, err
		}

		responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+utils.GetSHA1FromFile(responsePath))
		responseRecord.PayloadPath = responsePath
	} else {
		responseDump, err = httputil.DumpResponse(resp, true)
		if err != nil {
			return responsePath, err
		}

		responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+warc.GetSHA1(responseDump))
		responseRecord.Content = strings.NewReader(string(responseDump))
	}

	// Dump request
	requestDump, err = httputil.DumpRequestOut(resp.Request, true)
	if err != nil {
		if responsePath != "" {
			err := os.Remove(responsePath)
			if err != nil {
				logWarning.WithFields(logrus.Fields{
					"path":  responsePath,
					"error": err,
				}).Warning("Error deleting temporary file")
			}
		}
		return responsePath, err
	}

	// Initialize the request record
	var requestRecord = warc.NewRecord()
	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("WARC-Payload-Digest", "sha1:"+warc.GetSHA1(requestDump))
	requestRecord.Header.Set("WARC-Target-URI", resp.Request.URL.String())
	requestRecord.Header.Set("Host", resp.Request.URL.Host)
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	requestRecord.Content = strings.NewReader(string(requestDump))

	// Append records to the record batch
	batch.Records = append(batch.Records, responseRecord, requestRecord)

	c.WARCWriter <- batch

	return responsePath, nil
}
