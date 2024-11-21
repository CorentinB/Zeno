package stats

import (
	"sync/atomic"
	"testing"
)

func TestCounter_Incr(t *testing.T) {
	c := &counter{}

	// Increment the counter by 1
	c.incr(1)
	if atomic.LoadUint64(&c.count) != 1 {
		t.Errorf("expected count to be 1, got %d", atomic.LoadUint64(&c.count))
	}

	// Increment the counter by 5
	c.incr(5)
	if atomic.LoadUint64(&c.count) != 6 {
		t.Errorf("expected count to be 6, got %d", atomic.LoadUint64(&c.count))
	}
}

func TestCounter_Decr(t *testing.T) {
	c := &counter{}

	// Increment the counter by 10
	c.incr(10)
	if atomic.LoadUint64(&c.count) != 10 {
		t.Errorf("expected count to be 10, got %d", atomic.LoadUint64(&c.count))
	}

	// Decrement the counter by 3
	c.decr(3)
	if atomic.LoadUint64(&c.count) != 7 {
		t.Errorf("expected count to be 7, got %d", atomic.LoadUint64(&c.count))
	}

	// Decrement the counter by 7
	c.decr(7)
	if atomic.LoadUint64(&c.count) != 0 {
		t.Errorf("expected count to be 0, got %d", atomic.LoadUint64(&c.count))
	}
}

func TestCounter_Get(t *testing.T) {
	c := &counter{}

	// Increment the counter by 4
	c.incr(4)
	if c.get() != 4 {
		t.Errorf("expected count to be 4, got %d", c.get())
	}

	// Decrement the counter by 2
	c.decr(2)
	if c.get() != 2 {
		t.Errorf("expected count to be 2, got %d", c.get())
	}
}

func TestCounter_Reset(t *testing.T) {
	c := &counter{}

	// Increment the counter by 8
	c.incr(8)
	if atomic.LoadUint64(&c.count) != 8 {
		t.Errorf("expected count to be 8, got %d", atomic.LoadUint64(&c.count))
	}

	// Reset the counter
	c.reset()
	if atomic.LoadUint64(&c.count) != 0 {
		t.Errorf("expected count to be 0 after reset, got %d", atomic.LoadUint64(&c.count))
	}
}
