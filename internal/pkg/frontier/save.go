package frontier

import (
	"encoding/gob"
	"os"
	"path"

	"github.com/paulbellamy/ratecounter"
	"github.com/sirupsen/logrus"
)

type frontierStats struct {
	Hosts       map[string]*ratecounter.Counter
	QueuedCount int64
}

// Load take the path to the frontier's hosts pool and status dump
// it decodes that file and load it in the job's frontier
func (f *Frontier) Load() {
	// Open a RO file
	decodeFile, err := os.OpenFile(path.Join(f.JobPath, "frontier.gob"), os.O_RDONLY, 0644)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"error": err,
		}).Warning("Unable to load Frontier stats and host pool, it is not a problem if you are starting this job for the first time")
		return
	}
	defer decodeFile.Close()

	// Create a decoder
	decoder := gob.NewDecoder(decodeFile)

	// We create the structure to load the file's content
	var dump = new(frontierStats)
	dump.Hosts = make(map[string]*ratecounter.Counter, 0)

	// Decode the content of the file in the structure
	decoder.Decode(&dump)

	// Copy the loaded data to our actual frontier
	f.HostPool.Hosts = dump.Hosts

	logrus.WithFields(logrus.Fields{
		"hosts": len(f.HostPool.Hosts),
	}).Info("Successfully loaded previous frontier's hosts pool")
}

// Save write the in-memory hosts pool to resume properly the next time the job is loaded
func (f *Frontier) Save() {
	// Create a file for IO
	encodeFile, err := os.OpenFile(path.Join(f.JobPath, "frontier.gob"), os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		logrus.Warning(err)
	}
	defer encodeFile.Close()

	// We create the structure to save to the file,
	// it's a copy of the hosts pool and the count
	// of the queued items
	var dump = new(frontierStats)
	dump.Hosts = make(map[string]*ratecounter.Counter, 0)

	f.HostPool.Lock()
	dump.Hosts = f.HostPool.Hosts
	// Write to the file
	var encoder = gob.NewEncoder(encodeFile)
	if err := encoder.Encode(dump); err != nil {
		logrus.Warning(err)
	}
	f.HostPool.Unlock()
}
