package crawl

import (
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/internal/pkg/utils"
)

type WorkerPool struct {
	Crawl            *Crawl
	Count            uint
	Workers          sync.Map
	StopSignal       chan bool
	StopTimeout      time.Duration
	GarbageCollector chan uuid.UUID
}

func NewPool(count uint, stopTimeout time.Duration, crawl *Crawl) *WorkerPool {
	return &WorkerPool{
		Crawl:            crawl,
		Count:            count,
		Workers:          sync.Map{},
		StopSignal:       make(chan bool),
		StopTimeout:      stopTimeout,
		GarbageCollector: make(chan uuid.UUID),
	}
}

func (wp *WorkerPool) Start() {
	for i := uint(0); i < wp.Count; i++ {
		worker := wp.NewWorker(wp.Crawl)
		wp.Crawl.Log.Info("Starting worker", "worker", worker.ID)
		go worker.Run()
		go worker.WatchHang()
	}
	go wp.WorkerWatcher()
}

// WorkerWatcher is a background process that watches over the workers
// and remove them from the pool when they are done
func (wp *WorkerPool) WorkerWatcher() {
	for {
		select {

		// Stop the workers when requested
		// Then end the watcher
		case <-wp.StopSignal:
			wg := sync.WaitGroup{}
			wg.Add(wp.wpLen())

			wp.Workers.Range(func(key, value interface{}) bool {
				go func(worker *Worker) {
					wp.Crawl.Log.Info("Stopping worker", "worker", worker.ID)
					worker.Stop()
					wp.Workers.Delete(key)
				}(value.(*Worker))

				return true
			})

			wg.Wait()

			wp.Crawl.Log.Info("All workers are stopped, crawl/crawl.go:WorkerWatcher() is stopping")
			return

		// Check for finished workers marked for GC and remove them from the pool
		case UUID := <-wp.GarbageCollector:
			_, loaded := wp.Workers.LoadAndDelete(UUID.String())
			if !loaded {
				wp.Crawl.Log.Error("Worker marked for garbage collection not found in the pool", "worker", UUID)
				continue
			}
			wp.Crawl.Log.Info("Worker removed from the pool", "worker", UUID)
		}
	}
}

// EnsureFinished waits for all workers to finish
func (wp *WorkerPool) EnsureFinished() bool {
	var workerPoolLen int
	var timer = time.NewTimer(wp.StopTimeout)
	var sleep = time.Second * 10

	for {
		workerPoolLen = wp.wpLen()
		if workerPoolLen == 0 {
			return true
		}
		select {
		case <-timer.C:
			wp.Crawl.Log.Warn(fmt.Sprintf("[WORKERS] Timeout reached. %d workers still running", workerPoolLen))
			return false
		default:
			wp.Crawl.Log.Warn(fmt.Sprintf("[WORKERS] Waiting %s for %d workers to finish", sleep, workerPoolLen))
			time.Sleep(sleep)
		}
	}
}

// GetWorkerStateFromPool returns the state of a worker given its index in the worker pool
// if the provided index is -1 then the state of all workers is returned
func (wp *WorkerPool) GetWorkerStateFromPool(UUID string) interface{} {
	if UUID == "" {
		var workersStatus = new(APIWorkersState)
		wp.Workers.Range(func(_, value interface{}) bool {
			workersStatus.Workers = append(workersStatus.Workers, _getWorkerState(value.(*Worker)))
			return true
		})
		return workersStatus
	}
	worker, loaded := wp.Workers.Load(UUID)
	if !loaded {
		return nil
	}
	return _getWorkerState(worker.(*Worker))
}

func _getWorkerState(worker *Worker) *APIWorkerState {
	var URL, lastErr string
	isLocked := true

	if worker.TryLock() {
		isLocked = false
		worker.Unlock()
	}

	if worker.state.lastError != nil {
		lastErr = worker.state.lastError.Error()
	}

	if worker.state.currentItem != nil && worker.state.currentItem.URL != nil {
		URL = utils.URLToString(worker.state.currentItem.URL)
	}

	return &APIWorkerState{
		WorkerID:   worker.ID.String(),
		Status:     worker.state.status.String(),
		URL:        URL,
		LastSeen:   worker.state.lastSeen.Format(time.RFC3339),
		LastError:  lastErr,
		LastAction: worker.state.lastAction,
		Locked:     isLocked,
	}
}

func (wp *WorkerPool) wpLen() int {
	var length int
	wp.Workers.Range(func(_, _ interface{}) bool {
		length++
		return true
	})
	return length
}
