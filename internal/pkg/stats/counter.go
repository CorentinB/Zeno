package stats

import "sync/atomic"

type counter struct {
	count uint64
}

func (c *counter) incr(step uint64) {
	atomic.AddUint64(&c.count, step)
}

func (c *counter) decr(step uint64) {
	atomic.AddUint64(&c.count, ^uint64(step-1))
}

func (c *counter) get() uint64 {
	return atomic.LoadUint64(&c.count)
}

func (c *counter) reset() {
	atomic.StoreUint64(&c.count, 0)
}
