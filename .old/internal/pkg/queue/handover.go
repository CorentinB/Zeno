package queue

import (
	"sync/atomic"
	"time"
)

var defaultMonitorInterval = 20 * time.Millisecond

type handoverChannel struct {
	ch                  chan *handoverEncodedItem
	open                *atomic.Bool
	ready               *atomic.Bool
	drained             *atomic.Bool
	signalConsumerDone  chan bool
	activityTracker     *activityTracker
	activityTrackerSize int
	monitorInterval     *atomic.Value // stores time.Duration
}

type handoverEncodedItem struct {
	bytes []byte
	item  *Item
}

type activityTracker struct {
	buffer []*int64
	size   int
	head   atomic.Uint64
}

func newActivityTracker(size int) *activityTracker {
	buffer := make([]*int64, size)
	for i := range buffer {
		buffer[i] = new(int64)
	}
	return &activityTracker{
		buffer: buffer,
		size:   size,
	}
}

func (at *activityTracker) record(value int64) {
	head := at.head.Add(1) % uint64(at.size)
	atomic.StoreInt64(at.buffer[head], value)
}

func (at *activityTracker) sum() int64 {
	var sum int64
	for _, v := range at.buffer {
		sum += atomic.LoadInt64(v)
	}
	return sum
}

func newHandoverChannel() *handoverChannel {
	handover := &handoverChannel{
		ch:                 make(chan *handoverEncodedItem),
		open:               new(atomic.Bool),
		ready:              new(atomic.Bool),
		drained:            new(atomic.Bool),
		signalConsumerDone: make(chan bool),
		monitorInterval:    new(atomic.Value),
		activityTracker:    newActivityTracker(10),
	}
	handover.monitorInterval.Store(defaultMonitorInterval)
	handover.open.Store(false)
	close(handover.ch)
	close(handover.signalConsumerDone)
	return handover
}

func (h *handoverChannel) tryOpen(size int) bool {
	if !h.open.CompareAndSwap(false, true) {
		return false
	}
	h.ch = make(chan *handoverEncodedItem, size)
	h.signalConsumerDone = make(chan bool)
	h.ready.Store(true)
	h.drained.Store(false)
	go h.monitorActivity()
	return true
}

func (h *handoverChannel) tryClose() bool {
	if !h.open.CompareAndSwap(true, false) || h.ready.Load() || !h.drained.Load() {
		return false
	}
	select {
	case item := <-h.ch:
		if item != nil {
			panic("handover channel not drained") // This should never happen
		}
	default:
		break
	}
	close(h.ch)
	close(h.signalConsumerDone)
	return true
}

func (h *handoverChannel) tryPut(item *handoverEncodedItem) bool {
	if !h.ready.Load() || !h.open.Load() {
		return false
	}
	select {
	case h.ch <- item:
		return true
	default:
		return false
	}
}

func (h *handoverChannel) tryGet() (*handoverEncodedItem, bool) {
	if !h.ready.Load() || !h.open.Load() {
		return nil, false
	}
	select {
	case item := <-h.ch:
		if item == nil {
			return nil, true
		}
		h.activityTracker.record(1)
		return item, true
	default:
		return nil, false
	}
}

func (h *handoverChannel) tryDrain() ([]*handoverEncodedItem, bool) {
	if !h.open.Load() || h.ready.Load() {
		return nil, false
	}

	items := []*handoverEncodedItem{}
	ok := false
	for {
		select {
		case item := <-h.ch:
			if item == nil {
				continue
			}
			items = append(items, item)
			ok = true
		default:
			h.drained.Store(true)
			return items, ok
		}
	}
}

func (h *handoverChannel) monitorActivity() {
	for h.open.Load() {
		interval := h.monitorInterval.Load().(time.Duration)
		time.Sleep(interval)

		activity := h.activityTracker.sum()
		h.activityTracker.record(0)

		if activity == 0 {
			time.Sleep(interval)
			if h.activityTracker.sum() == 0 {
				h.signalConsumerDone <- true
				h.ready.Store(false)
				return
			}
		}

		// Adjust monitoring interval based on activity
		if activity > 10 {
			h.monitorInterval.Store(10 * time.Millisecond)
		} else if activity <= 10 && activity > 1 {
			h.monitorInterval.Store(30 * time.Millisecond)
		} else if activity <= 1 {
			h.monitorInterval.Store(75 * time.Millisecond)
		}
	}
}

func (q *PersistentGroupedQueue) TempDisableHandover(enableBack chan struct{}, syncHandover chan struct{}) bool {
	if !q.useHandover.CompareAndSwap(true, false) {
		return false
	}

	syncHandover <- struct{}{}

	for {
		timeout := time.After(1 * time.Minute)
		select {
		case <-enableBack:
			ok := q.useHandover.CompareAndSwap(false, true)
			syncHandover <- struct{}{}
			return ok
		case <-timeout:
			return false
		}
	}
}
