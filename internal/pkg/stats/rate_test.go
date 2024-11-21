package stats

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRate_Start(t *testing.T) {
	rate := &rate{}

	// Increment the rate counter
	rate.incr(5)

	// Wait for more than a second to allow the ticker to update the rate
	time.Sleep(1100 * time.Millisecond)

	// Check if the rate per second is correctly updated
	if rate.get() != 5 {
		t.Errorf("expected rate per second to be 5, got %d", rate.get())
	}

	// Increment the rate counter again
	rate.incr(10)

	// Wait for more than a second to allow the ticker to update the rate
	time.Sleep(1100 * time.Millisecond)

	// Check if the rate per second is correctly updated
	if rate.get() != 10 {
		t.Errorf("expected rate per second to be 10, got %d", rate.get())
	}

	// Increment the rate counter multiple times and check the rate over several seconds
	for i := 0; i < 5; i++ {
		rate.incr(2)
		time.Sleep(1100 * time.Millisecond)
		expectedRate := uint64(2)
		if rate.get() != expectedRate {
			t.Errorf("expected rate per second to be %d, got %d", expectedRate, rate.get())
		}
	}
}

func TestRate_Incr(t *testing.T) {
	rate := &rate{}

	// Increment the rate counter
	rate.incr(3)

	// Check if the count is correctly incremented
	if atomic.LoadUint64(&rate.count) != 3 {
		t.Errorf("expected count to be 3, got %d", atomic.LoadUint64(&rate.count))
	}

	// Increment the rate counter again
	rate.incr(2)

	// Check if the count is correctly incremented
	if atomic.LoadUint64(&rate.count) != 5 {
		t.Errorf("expected count to be 5, got %d", atomic.LoadUint64(&rate.count))
	}
}

func TestRate_Get(t *testing.T) {
	rate := &rate{}

	// Increment the rate counter
	rate.incr(7)

	// Wait for more than a second to allow the ticker to update the rate
	time.Sleep(1100 * time.Millisecond)

	// Check if the rate per second is correctly updated
	if rate.get() != 7 {
		t.Errorf("expected rate per second to be 7, got %d", rate.get())
	}
}

func TestRate_GetTotal(t *testing.T) {
	rate := &rate{}

	// Increment the rate counter
	rate.incr(7)

	// Check if the total count is correctly retrieved
	if rate.getTotal() != 7 {
		t.Errorf("expected total count to be 7, got %d", rate.getTotal())
	}
}
