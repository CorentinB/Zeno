package frontier

import (
	"path"
	"sync"

	"github.com/CorentinB/Zeno/internal/pkg/utils"
	"github.com/beeker1121/goque"
	"github.com/paulbellamy/ratecounter"
	"github.com/philippgille/gokv/leveldb"
	"github.com/sirupsen/logrus"
)

var logInfo *logrus.Logger
var logWarning *logrus.Logger

// Frontier holds all the data for a frontier
type Frontier struct {
	FinishingQueueWriter *utils.TAtomBool
	FinishingQueueReader *utils.TAtomBool
	IsQueueWriterActive  *utils.TAtomBool
	IsQueueReaderActive  *utils.TAtomBool
	JobPath              string

	// PullChan and PushChan are respectively the channels used for workers
	// to get new URLs to archive, and the channel to push the discovered URLs
	// to the frontier
	PullChan chan *Item
	PushChan chan *Item

	// Queue is a local queue storing all the URLs to crawl
	// it's a prefixed queue, basically one sub-queue per host
	Queue *goque.PrefixQueue
	// QueueCount store the number of URLs currently queued
	QueueCount *ratecounter.Counter

	// HostPool is an struct that contains a map and a Mutex.
	// the map contains all the different hosts that Zeno crawled,
	// with a counter for each, going through that map gives us
	// the prefix to query from the queue
	HostPool *HostPool

	UseSeencheck bool
	Seencheck    *Seencheck
}

// Init ininitialize the components of a frontier
func (f *Frontier) Init(jobPath string, logInf, logWarn *logrus.Logger, workers int, useSeencheck bool) (err error) {
	f.JobPath = jobPath

	logInfo = logInf
	logWarning = logWarn

	// Initialize host pool
	f.HostPool = new(HostPool)
	f.HostPool.Mutex = new(sync.Mutex)
	f.HostPool.Hosts = make(map[string]*ratecounter.Counter, 0)

	// Initialize the frontier channels
	f.PullChan = make(chan *Item, workers)
	f.PushChan = make(chan *Item, workers)

	// Initialize the queue
	f.Queue, err = newPersistentQueue(jobPath)
	if err != nil {
		return err
	}
	f.QueueCount = new(ratecounter.Counter)
	f.QueueCount.Incr(int64(f.Queue.Length()))
	logrus.Info("Persistent queue initialized")

	// Initialize the seencheck
	f.UseSeencheck = useSeencheck
	if f.UseSeencheck {
		f.Seencheck = new(Seencheck)
		f.Seencheck.SeenCount = new(ratecounter.Counter)
		f.Seencheck.SeenDB, err = leveldb.NewStore(leveldb.Options{Path: path.Join(jobPath, "seencheck")})
		if err != nil {
			return err
		}
		logrus.Info("Seencheck initialized")
	}

	f.FinishingQueueReader = new(utils.TAtomBool)
	f.FinishingQueueWriter = new(utils.TAtomBool)
	f.IsQueueReaderActive = new(utils.TAtomBool)
	f.IsQueueWriterActive = new(utils.TAtomBool)

	return nil
}

// Start fire up the background processes that handle the frontier
func (f *Frontier) Start() {
	// Function responsible for writing the items push on PushChan to the
	// local queue, items received on this channels are typically initial seeds
	// or outlinks discovered on web pages
	go f.writeItemsToQueue()

	// Function responsible for reading the items from the queue and dispatching
	// them to the workers listening on PullChan
	go f.readItemsFromQueue()
}
