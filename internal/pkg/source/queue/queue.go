// Package queue provides a persistent grouped queue implementation when crawling without an external crawling orchestrator.
package queue

import (
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sync"
	"sync/atomic"

	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/source/queue/index"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

// PersistentGroupedQueue is a persistent grouped queue implementation that can be used to store and retrieve items.
type PersistentGroupedQueue struct {
	Paused    *utils.TAtomBool
	Empty     *utils.TAtomBool
	closed    *utils.TAtomBool
	finishing *utils.TAtomBool

	queueDirPath    string
	queueFile       *os.File
	metadataFile    *os.File
	metadataEncoder *gob.Encoder
	metadataDecoder *gob.Decoder
	index           *index.IndexManager
	stats           *QueueStats
	statsMutex      sync.Mutex // Write lock for stats
	currentHost     *atomic.Uint64
	mutex           sync.RWMutex
}

// Item represents an item in the queue. It is different from the models.Item struct as it contains additional fields.
type Item struct {
	URL             *url.URL
	ParentURL       *url.URL
	Hop             uint64
	Type            string
	ID              string
	BypassSeencheck bool
	Hash            uint64
	LocallyCrawled  uint64
	Redirect        uint64
}

func init() {
	log.Start()
}

// NewPersistentGroupedQueue creates a new persistent grouped queue.
func NewPersistentGroupedQueue(queueDirPath string) (*PersistentGroupedQueue, error) {
	err := os.MkdirAll(queueDirPath, 0755)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path.Join(queueDirPath, "queue"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open queue file: %w", err)
	}

	metafile, err := os.OpenFile(path.Join(queueDirPath, "queue.meta"), os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open metadata file: %w", err)
	}

	indexManager, err := index.NewIndexManager(path.Join(queueDirPath, "index_wal"), path.Join(queueDirPath, "index"), queueDirPath)
	if err != nil {
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	q := &PersistentGroupedQueue{
		Paused: new(utils.TAtomBool),
		Empty:  new(utils.TAtomBool),

		closed:    new(utils.TAtomBool),
		finishing: new(utils.TAtomBool),

		queueDirPath:    queueDirPath,
		queueFile:       file,
		metadataFile:    metafile,
		metadataEncoder: gob.NewEncoder(metafile),
		metadataDecoder: gob.NewDecoder(metafile),
		index:           indexManager,
		currentHost:     new(atomic.Uint64),
		stats: &QueueStats{
			elementsPerHost: make(map[string]int),
		},
	}

	// Set the queue as not paused and current host to 0
	// Set the queue as not empty considering that it might have items at the beginning when resuming, the first dequeue will update it to true if needed
	q.Empty.Set(false)
	q.Paused.Set(false)
	q.closed.Set(false)
	q.finishing.Set(false)
	q.currentHost.Store(0)

	// Loading stats from the disk means deleting the file from disk after having read it
	if err = q.loadStatsFromFile(path.Join(q.queueDirPath, "queue.stats")); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("load queue stats: %w", err)
		}
	}

	if err = q.loadMetadata(); err != nil {
		q.Close()
		return nil, fmt.Errorf("load queue metadata: %w", err)
	}

	return q, nil
}

// Close closes the queue and saves the metadata.
func (q *PersistentGroupedQueue) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if q.closed.Get() {
		return ErrQueueAlreadyClosed
	}

	q.finishing.Set(true)
	q.closed.Set(true)

	// Save metadata
	err := q.saveStatsToFile(path.Join(q.queueDirPath, "queue.stats"))
	if err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Close the metadata file
	err = q.metadataFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close metadata file: %w", err)
	}

	// Close the main queue file
	err = q.queueFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close queue file: %w", err)
	}

	// Close the index manager
	err = q.index.Close()
	if err != nil {
		return fmt.Errorf("failed to close index manager: %w", err)
	}

	return nil
}

// FreezeDequeue freezes the dequeue operation, meaning that no more items will be dequeued from the queue.
func (q *PersistentGroupedQueue) FreezeDequeue() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.finishing.Set(true)
}
