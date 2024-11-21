package stats

import (
	"sync/atomic"
	"time"
)

type rate struct {
	count      uint64
	lastCount  uint64
	lastUpdate int64
}

func (rps *rate) incr(step uint64) {
	atomic.AddUint64(&rps.count, step)
}

func (rps *rate) get() uint64 {
	now := time.Now().Unix()
	lastUpdate := atomic.LoadInt64(&rps.lastUpdate)

	if now == lastUpdate {
		return atomic.LoadUint64(&rps.lastCount)
	}

	currentCount := atomic.LoadUint64(&rps.count)
	lastCount := atomic.SwapUint64(&rps.count, 0)
	atomic.StoreUint64(&rps.lastCount, lastCount)
	atomic.StoreInt64(&rps.lastUpdate, now)

	return currentCount
}

func (rps *rate) getTotal() uint64 {
	return atomic.LoadUint64(&rps.count)
}

func (rps *rate) reset() {
	atomic.StoreUint64(&rps.count, 0)
	atomic.StoreUint64(&rps.lastCount, 0)
	atomic.StoreInt64(&rps.lastUpdate, 0)
}
