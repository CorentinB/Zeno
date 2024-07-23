package index

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"
)

func (im *IndexManager) periodicDump(errChan chan error, stop chan struct{}) {
	for {
		select {
		case <-im.dumpTicker.C:
			if err := im.performDump(); err != nil {
				// Log the error and exit
				errChan <- fmt.Errorf("failed to perform periodic dump: %w", err)
			}
		case <-stop:
			return
		}
	}
}

// performDump writes the current index to the index file and truncates the WAL file
// we consider 3 instances of index file: temp index file (n), old index file (n-2), and actual index file (n-1)
// the process is as follows : n is created, n-2 is deleted, n-1 is renamed to n-2, n is renamed to n-1
func (im *IndexManager) performDump() error {
	im.Lock()
	defer im.Unlock()

	// Create a temporary file for the new index dump
	tempFile, err := os.CreateTemp("", "index_dump_")
	if err != nil {
		return fmt.Errorf("failed to create temp file for index dump: %w", err)
	}
	defer tempFile.Close()

	// Dump the current index to the temporary file
	if err := im.dumpIndexToFile(tempFile); err != nil {
		return fmt.Errorf("failed to dump index to temp file: %w", err)
	}

	// Remove any previous backup index file
	if err := os.Remove(im.indexFile.Name() + ".old"); err != nil {
		return fmt.Errorf("failed to remove old index file: %w", err)
	}

	// Backup the current index file by renaming it
	if err := os.Rename(im.indexFile.Name(), im.indexFile.Name()+".old"); err != nil {
		return fmt.Errorf("failed to backup index file: %w", err)
	}

	// Move the temporary file to the actual index file
	if err := os.Rename(tempFile.Name(), im.indexFile.Name()); err != nil {
		// Try to rollback if rename fails
		os.Rename(im.indexFile.Name()+".old", im.indexFile.Name())
		return fmt.Errorf("failed to rename temp file to index file: %w", err)
	}

	// Truncate the WAL file
	if err := im.walFile.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}
	if _, err := im.walFile.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek WAL file: %w", err)
	}

	im.opsSinceDump = 0
	im.lastDumpTime = time.Now()

	return nil
}

func (im *IndexManager) dumpIndexToFile(file *os.File) error {
	// Encode the current index to the temporary file
	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(im.hostIndex); err != nil {
		return fmt.Errorf("failed to encode hostIndex to temp file: %w", err)
	}

	// Sync the temporary file to ensure it's written to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	return nil
}

func (im *IndexManager) loadIndex() error {
	// Try to load the index from the index file
	if err := im.indexDecoder.Decode(&im.hostIndex); err != nil {
		if err != io.EOF {
			return fmt.Errorf("failed to decode index: %w", err)
		}
		// If the file is empty (EOF), initialize an empty index
		im.hostIndex = newIndex()
	}

	return nil
}
