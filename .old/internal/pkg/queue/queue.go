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
	"github.com/internetarchive/Zeno/internal/pkg/queue/index"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

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

	useHandover   *atomic.Bool
	handover      *handoverChannel
	HandoverOpen  *utils.TAtomBool
	handoverMutex sync.Mutex
	handoverCount *atomic.Uint64

	useCommit      bool
	enqueueOp      func(*Item) error
	batchEnqueueOp func(...*Item) error
	dequeueOp      func() (*Item, error)

	logger *log.Logger
}

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

func NewPersistentGroupedQueue(queueDirPath string, useHandover bool, useCommit bool) (*PersistentGroupedQueue, error) {
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

	indexManager, err := index.NewIndexManager(path.Join(queueDirPath, "index_wal"), path.Join(queueDirPath, "index"), queueDirPath, useCommit)
	if err != nil {
		return nil, fmt.Errorf("create index manager: %w", err)
	}

	q := &PersistentGroupedQueue{
		Paused: new(utils.TAtomBool),
		Empty:  new(utils.TAtomBool),

		closed:    new(utils.TAtomBool),
		finishing: new(utils.TAtomBool),

		useHandover:   new(atomic.Bool),
		HandoverOpen:  new(utils.TAtomBool),
		handoverCount: new(atomic.Uint64),

		useCommit: useCommit,

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

	// Logging
	logger, _ := log.DefaultOrStored()
	q.logger = logger

	// Set the queue as not paused and current host to 0
	// Set the queue as not empty considering that it might have items at the beginning when resuming, the first dequeue will update it to true if needed
	q.Empty.Set(false)
	q.Paused.Set(false)
	q.closed.Set(false)
	q.finishing.Set(false)
	q.currentHost.Store(0)

	// Handover
	q.useHandover.Store(useHandover)
	q.HandoverOpen.Set(false)
	q.handoverCount.Store(0)
	if useHandover {
		q.handover = newHandoverChannel()
	}

	// Commit
	if useCommit {
		q.enqueueOp = q.enqueueUntilCommitted
		q.batchEnqueueOp = q.batchEnqueueUntilCommitted
		q.dequeueOp = q.dequeueCommitted
	} else {
		q.enqueueOp = q.enqueueNoCommit
		q.batchEnqueueOp = q.batchEnqueueNoCommit
		q.dequeueOp = q.dequeueNoCommit
	}

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

func (q *PersistentGroupedQueue) FreezeDequeue() {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.finishing.Set(true)
}
