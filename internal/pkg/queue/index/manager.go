package index

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"
	"time"
)

var dumpFrequency = 60 // seconds

type Operation int

const (
	OpAdd Operation = iota
	OpPop
)

type WALEntry struct {
	Op       Operation
	Host     string
	BlobID   string
	Position uint64
	Size     uint64
}

type IndexManager struct {
	sync.Mutex
	hostIndex    *Index
	walFile      *os.File
	indexFile    *os.File
	walEncoder   *gob.Encoder
	walDecoder   *gob.Decoder
	indexEncoder *gob.Encoder
	indexDecoder *gob.Decoder
	dumpTicker   *time.Ticker
	lastDumpTime time.Time
	opsSinceDump int
	totalOps     uint64
	stopChan     chan struct{}
}

// NewIndexManager creates a new IndexManager instance and loads the index from the index file.
func NewIndexManager(walPath, indexPath string) (*IndexManager, error) {
	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		walFile.Close()
		return nil, fmt.Errorf("failed to open index file: %w", err)
	}

	im := &IndexManager{
		hostIndex:    newIndex(),
		walFile:      walFile,
		indexFile:    indexFile,
		walEncoder:   gob.NewEncoder(walFile),
		walDecoder:   gob.NewDecoder(walFile),
		indexEncoder: gob.NewEncoder(indexFile),
		indexDecoder: gob.NewDecoder(indexFile),
		dumpTicker:   time.NewTicker(time.Duration(dumpFrequency) * time.Second),
		lastDumpTime: time.Now(),
		stopChan:     make(chan struct{}),
	}

	// Check if WAL file is empty
	im.Lock()
	empty, err := im.unsafeIsWALEmpty()
	im.Unlock()
	if !empty {
		err := im.RecoverFromCrash()
		if err != nil {
			walFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to recover from crash: %w", err)
		}
	} else {
		err = im.loadIndex()
		if err != nil {
			walFile.Close()
			indexFile.Close()
			return nil, fmt.Errorf("failed to load index: %w", err)
		}
	}

	periodicDumpStopChan := make(chan struct{})
	periodicDumpErrChan := make(chan error)
	go func(im *IndexManager, errChan chan error, stopChan chan struct{}) {
		for {
			select {
			case stop := <-im.stopChan:
				periodicDumpStopChan <- stop
			case err := <-errChan:
				if err != nil {
					fmt.Printf("Periodic dump failed: %v", err) // No better way to log this, will wait for https://github.com/internetarchive/Zeno/issues/92
				}
			}
		}
	}(im, periodicDumpErrChan, periodicDumpStopChan)

	go im.periodicDump(periodicDumpErrChan, periodicDumpStopChan)

	return im, nil
}

func (im *IndexManager) Add(host string, id string, position uint64, size uint64) error {
	im.Lock()
	defer im.Unlock()

	// Write to WAL
	err := im.unsafeWriteToWAL(OpAdd, host, id, position, size)
	if err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	// Update in-memory index
	if err := im.hostIndex.add(host, id, position, size); err != nil {
		return fmt.Errorf("failed to update in-memory index: %w", err)
	}

	im.opsSinceDump++
	im.totalOps++

	return nil
}

// Pop removes the oldest blob from the specified host's queue and returns its ID, position, and size.
// Pop is responsible for synchronizing the pop of the blob from the in-memory index and writing to the WAL.
// First it starts a goroutine that waits for the to-be-popped blob infos through blobChan, then writes to the WAL and if successful
// informs index.pop() through WALSuccessChan to either continue as normal or return an error.
func (im *IndexManager) Pop(host string) (id string, position uint64, size uint64, err error) {
	im.Lock()
	defer im.Unlock()

	// Prepare the channels
	blobChan := make(chan *blob)
	WALSuccessChan := make(chan bool)
	errChan := make(chan error)

	go func() {
		// Write to WAL
		blob := <-blobChan
		err := im.unsafeWriteToWAL(OpPop, host, blob.id, blob.position, blob.size)
		if err != nil {
			errChan <- fmt.Errorf("failed to write to WAL: %w", err)
			WALSuccessChan <- false
		}
		id = blob.id
		position = blob.position
		size = blob.size
		WALSuccessChan <- true
		errChan <- nil
	}()

	// Pop from in-memory index
	err = im.hostIndex.pop(host, blobChan, WALSuccessChan)
	if err != nil {
		return "", 0, 0, err
	}

	if err := <-errChan; err != nil {
		return "", 0, 0, err
	}

	im.opsSinceDump++
	im.totalOps++

	close(blobChan)
	close(WALSuccessChan)
	close(errChan)

	return
}

func (im *IndexManager) Close() error {
	im.dumpTicker.Stop()
	im.stopChan <- struct{}{}
	if err := im.performDump(); err != nil {
		return fmt.Errorf("failed to perform final dump: %w", err)
	}
	if err := im.walFile.Close(); err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}
	if err := im.indexFile.Close(); err != nil {
		return fmt.Errorf("failed to close index file: %w", err)
	}
	return nil
}

func (im *IndexManager) GetStats() string {
	im.Lock()
	defer im.Unlock()

	return fmt.Sprintf("Total operations: %d, Operations since last dump: %d",
		im.totalOps, im.opsSinceDump)
}

// GetHosts returns a list of all hosts in the index
func (im *IndexManager) GetHosts() []string {
	im.Lock()
	defer im.Unlock()

	return im.hostIndex.getOrderedHosts()
}

func (im *IndexManager) IsEmpty() bool {
	im.Lock()
	defer im.Unlock()

	im.hostIndex.Lock()
	defer im.hostIndex.Unlock()

	if len(im.hostIndex.index) == 0 {
		return true
	}
	return false
}
