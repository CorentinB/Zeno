package crawl

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/internal/pkg/queue"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

const (
	// B represent a Byte
	B = 1
	// KB represent a Kilobyte
	KB = 1024 * B
	// MB represent a MegaByte
	MB = 1024 * KB
	// GB represent a GigaByte
	GB = 1024 * MB
)

type status int

const (
	idle status = iota
	processing
	completed
)

func (s status) String() string {
	statusStr := map[status]string{
		idle:       "idle",
		processing: "processing",
		completed:  "completed",
	}
	return statusStr[s]
}

type workerState struct {
	currentItem  *queue.Item
	previousItem *queue.Item
	status       status
	lastAction   string
	lastError    error
	lastSeen     time.Time
}

type Worker struct {
	sync.Mutex
	ID         uuid.UUID
	state      *workerState
	doneSignal chan bool
	pool       *WorkerPool
	logger     *log.FieldedLogger
}

// Run is the key component of a crawl, it's a background processed dispatched
// when the crawl starts, it listens on a channel to get new URLs to archive,
// and eventually push newly discovered URLs back in the queue.
func (w *Worker) Run() {
	// Start archiving the URLs!
	for {
		// Check if the crawl is paused or needs to be stopped
		select {
		case <-w.doneSignal:
			w.Lock()
			w.state.currentItem = nil
			w.state.status = completed
			w.logger.Info("Worker stopped")
			return
		default:
			for w.pool.Crawl.Paused.Get() {
				w.state.lastAction = "waiting for crawl to resume"
				time.Sleep(10 * time.Millisecond)
			}
			for w.pool.Crawl.Queue.Empty.Get() && !w.pool.Crawl.Queue.HandoverOpen.Get() {
				w.state.lastAction = fmt.Sprintf("waiting for queue to be filled, queue empty: %t, handover open: %t", w.pool.Crawl.Queue.Empty.Get(), w.pool.Crawl.Queue.HandoverOpen.Get())
				time.Sleep(5 * time.Millisecond)
			}
			if !w.pool.Crawl.Queue.CanDequeue() {
				w.state.lastAction = "waiting for queue to be dequeuable"
				time.Sleep(10 * time.Millisecond)
				continue
			}
		}

		w.state.lastAction = "dequeueing item"
		// Try to get an item from the queue or handover channel
		item, err := w.pool.Crawl.Queue.Dequeue()
		if err != nil {
			// Log the error too?
			w.state.lastError = err
			switch err {
			case queue.ErrQueueEmpty:
				w.state.lastAction = "queue is empty"
			case queue.ErrQueueClosed:
				w.state.lastAction = "queue is closed"
			case queue.ErrDequeueClosed:
				w.state.lastAction = "dequeue is closed"
			default:
				w.state.lastAction = "unhandled dequeue error"
			}
			continue
		}

		// Can it happen? I don't think so but let's be safe
		if item == nil {
			continue
		}
		w.Lock()
		w.state.lastAction = "got item"

		// If the host of the item is in the host exclusion list, we skip it
		if utils.StringInSlice(item.URL.Host, w.pool.Crawl.ExcludedHosts) || !w.pool.Crawl.isHostIncluded(item.URL) {
			if w.pool.Crawl.UseHQ {
				w.state.lastAction = "skipping item because of host exclusion"
				// If we are using the HQ, we want to mark the item as done
				w.pool.Crawl.HQFinishedChannel <- item
			}
			w.Unlock()
			continue
		}

		// Launches the capture of the given item
		w.state.lastAction = "starting capture"
		w.unsafeCapture(item)
		w.Unlock()
	}
}

// unsafeCapture is named like so because it should only be called when the worker is locked
func (w *Worker) unsafeCapture(item *queue.Item) {
	if item == nil {
		return
	}

	// Signals that the worker is processing an item
	w.pool.Crawl.ActiveWorkers.Incr(1)
	w.state.currentItem = item
	w.state.status = processing

	// Capture the item
	w.state.lastAction = "capturing item"
	w.state.lastError = w.pool.Crawl.Capture(item)

	// Signals that the worker has finished processing the item
	w.state.lastAction = "finished capturing"
	w.state.status = idle
	w.state.currentItem = nil
	w.state.previousItem = item
	w.pool.Crawl.ActiveWorkers.Incr(-1)
	w.state.lastSeen = time.Now()
}

func (w *Worker) Stop() {
	w.doneSignal <- true
	for w.state.status != completed {
		time.Sleep(10 * time.Millisecond)
	}
}

func (wp *WorkerPool) NewWorker(crawlParameters *Crawl) *Worker {
	UUID := uuid.New()
	worker := &Worker{
		ID: UUID,
		logger: crawlParameters.Log.WithFields(map[string]interface{}{
			"worker": UUID,
		}), // This is a bit weird but it provides every worker with a logger that has the worker UUID
		state: &workerState{
			status:       idle,
			previousItem: nil,
			currentItem:  nil,
			lastError:    nil,
		},
		doneSignal: make(chan bool, 1),
		pool:       wp,
	}

	_, loaded := wp.Workers.LoadOrStore(UUID, worker)
	if loaded {
		panic("Worker UUID already exists, wtf?")
	}

	return worker
}

// WatchHang is a function that checks if a worker is hanging based on the last time it was seen
func (w *Worker) WatchHang() {
	w.logger.Info("Starting worker deadlock watcher")
	for {
		tryLockCounter := 0
		time.Sleep(5 * time.Second)
		for !w.TryLock() {
			time.Sleep(1 * time.Second)
			if tryLockCounter > 10 && w.state.status != completed && w.state.status != processing {
				w.logger.Error("Worker is deadlocked, trying to stop it", "status", w.state.status, "last_seen", w.state.lastSeen, "last_action", w.state.lastAction)
				w.Stop()
				return
			} else if w.state.status != completed && w.state.status != processing {
				tryLockCounter++
			} else {
				tryLockCounter = 0
			}
		}
		// This is commented out because it's not working as expected
		// if w.state.status != idle && time.Since(w.state.lastSeen) > 10*time.Second {
		// 	w.logger.Warn("Worker is hanging, stopping it")
		// 	w.Unlock()
		// 	w.Stop()
		// 	return
		// }
		w.Unlock()
	}
}
