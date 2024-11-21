package queue

import (
	"encoding/json"
	"os"
	"time"
)

type QueueStats struct {
	FirstEnqueueTime           time.Time      `json:"first_enqueue_time"`
	LastEnqueueTime            time.Time      `json:"last_enqueue_time"`
	FirstDequeueTime           time.Time      `json:"first_dequeue_time"`
	LastDequeueTime            time.Time      `json:"last_dequeue_time"`
	elementsPerHost            map[string]int `json:"-"` // do not access it without locking statsMutex
	TotalElements              int            `json:"total_elements"`
	UniqueHosts                int            `json:"unique_hosts"`
	EnqueueCount               int            `json:"enqueue_count"`
	DequeueCount               int            `json:"dequeue_count"`
	AverageTimeBetweenEnqueues time.Duration  `json:"average_time_between_enqueues"`
	AverageTimeBetweenDequeues time.Duration  `json:"average_time_between_dequeues"`
	AverageElementsPerHost     float64        `json:"average_elements_per_host"`
	HandoverSuccessGetCount    uint64         `json:"handover_success_get_count"`
}

// generate and return the snapshot of the queue stats
// NOTE: elementsPerHost is not included in the snapshot
func (q *PersistentGroupedQueue) GetStats() QueueStats {
	q.statsMutex.Lock()
	defer q.statsMutex.Unlock()
	q.genStats()

	// hack to avoid copying the map
	elementsPerHost := q.stats.elementsPerHost
	q.stats.elementsPerHost = nil
	defer func() {
		q.stats.elementsPerHost = elementsPerHost
	}()

	return *q.stats
}

// GetElementsPerHost is not thread-safe and should be called with the statsMutex locked
// If you real need to access elementsPerHost, you should lock the statsMutex
func (q *PersistentGroupedQueue) GetElementsPerHost() *map[string]int {
	return &q.stats.elementsPerHost
}

// genStats is not thread-safe and should be called with the statsMutex locked
func (q *PersistentGroupedQueue) genStats() {
	if q.stats.UniqueHosts > 0 {
		q.stats.AverageElementsPerHost = float64(q.stats.TotalElements) / float64(q.stats.UniqueHosts)
	} else {
		q.stats.AverageElementsPerHost = 0
	}

	if q.stats.DequeueCount > 0 {
		q.stats.AverageTimeBetweenDequeues = time.Since(q.stats.FirstDequeueTime) / time.Duration(q.stats.DequeueCount)
	}
	if q.stats.EnqueueCount > 0 {
		q.stats.AverageTimeBetweenEnqueues = time.Since(q.stats.FirstEnqueueTime) / time.Duration(q.stats.EnqueueCount)
	}

	// Calculate handover success get count
	if q.useHandover.Load() {
		q.stats.HandoverSuccessGetCount = q.handoverCount.Load()
	}
}

func (q *PersistentGroupedQueue) loadStatsFromFile(path string) error {
	q.statsMutex.Lock()
	defer q.statsMutex.Unlock()

	// Load the stats from the file
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Decode the stats
	err = json.NewDecoder(f).Decode(q.stats)
	if err != nil {
		return err
	}

	// Delete the file after reading it
	err = os.Remove(path)
	if err != nil {
		return err
	}

	return nil
}

func (q *PersistentGroupedQueue) saveStatsToFile(path string) error {
	// Save the stats to the file, creating it if it doesn't exist or truncating it if it does
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	// Encode the stats
	err = json.NewEncoder(f).Encode(q.GetStats())
	if err != nil {
		return err
	}

	return nil
}

func (q *PersistentGroupedQueue) updateDequeueStats(host string) {
	q.statsMutex.Lock()
	defer q.statsMutex.Unlock()

	q.stats.TotalElements--
	q.stats.elementsPerHost[host]--
	if q.stats.DequeueCount == 0 {
		q.stats.FirstDequeueTime = time.Now()
	}
	q.stats.DequeueCount++
	q.stats.LastDequeueTime = time.Now()
	if q.stats.elementsPerHost[host] == 0 {
		delete(q.stats.elementsPerHost, host)
		q.stats.UniqueHosts--
	}
}

func (q *PersistentGroupedQueue) updateEnqueueStats(item *Item) {
	q.statsMutex.Lock()
	defer q.statsMutex.Unlock()

	q.stats.TotalElements++
	if q.stats.elementsPerHost[item.URL.Host] == 0 {
		q.stats.UniqueHosts++ // Increment UniqueHosts when we see a new host
	}
	q.stats.elementsPerHost[item.URL.Host]++
	if q.stats.EnqueueCount == 0 {
		q.stats.FirstEnqueueTime = time.Now()
	}
	q.stats.EnqueueCount++
	q.stats.LastEnqueueTime = time.Now()
}
