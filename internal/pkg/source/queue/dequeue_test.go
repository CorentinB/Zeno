package queue

import (
	"fmt"
	"net/url"
	"os"
	"testing"
)

func TestDequeue(t *testing.T) {
	// Create a temporary directory for the queue files
	tempDir, err := os.MkdirTemp("", "queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new queue
	q, err := NewPersistentGroupedQueue(tempDir)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue some items
	url1, _ := url.Parse("http://example1.com")
	url2, _ := url.Parse("http://example2.com")
	url3, _ := url.Parse("http://example1.com/page")

	item1, err := NewItem(url1, nil, "test", 0, "1", false)
	if err != nil {
		t.Fatalf("Failed to create item: %v", err)
	}

	item2, err := NewItem(url2, nil, "test", 0, "2", false)
	if err != nil {
		t.Fatalf("Failed to create item: %v", err)
	}

	item3, err := NewItem(url3, nil, "test", 0, "3", false)
	if err != nil {
		t.Fatalf("Failed to create item: %v", err)
	}

	items := []*Item{item1, item2, item3}

	for _, item := range items {
		err = q.Enqueue(item)
		if err != nil {
			t.Fatalf("Failed to enqueue item: %v", err)
		}
	}

	// Test dequeue
	for i := 0; i < 3; i++ {
		dequeued, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Failed to dequeue item: %v", err)
		}
		if dequeued == nil {
			t.Fatalf("Dequeued item is nil")
		}
		// Check if the dequeued item matches one of the enqueued items
		found := false
		for _, item := range items {
			if dequeued.ID == item.ID {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("Dequeued unexpected item: %v", dequeued)
		}
	}

	// Test dequeue on closed queue
	q.Close()
	_, err = q.Dequeue()
	if err != ErrDequeueClosed {
		t.Fatalf("Expected ErrDequeueClosed, got %v", err)
	}
}

func TestDequeueHostOrder(t *testing.T) {
	// Create a temporary directory for the queue files
	tempDir, err := os.MkdirTemp("", "queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a new queue
	q, err := NewPersistentGroupedQueue(tempDir)
	if err != nil {
		t.Fatalf("Failed to create queue: %v", err)
	}
	defer q.Close()

	// Enqueue items from different hosts
	urls := []string{
		"http://example1.com/1",
		"http://example2.com/1",
		"http://example3.com/1",
		"http://example1.com/2",
		"http://example2.com/2",
		"http://example3.com/2",
	}

	for i, urlStr := range urls {
		url, _ := url.Parse(urlStr)
		item, err := NewItem(url, nil, "test", 0, fmt.Sprint(i), false)
		if err != nil {
			t.Fatalf("Failed to create item: %v", err)
		}
		err = q.Enqueue(item)
		if err != nil {
			t.Fatalf("Failed to enqueue item: %v", err)
		}
	}

	// Expected hosts corresponding to the order of items enqueued
	expectedHosts := []string{
		"example1.com",
		"example2.com",
		"example3.com",
		"example1.com",
		"example2.com",
		"example3.com",
	}

	for i, expectedHost := range expectedHosts {
		dequeued, err := q.Dequeue()
		if err != nil {
			t.Fatalf("Failed to dequeue item: %v", err)
		}
		if dequeued.URL.Host != expectedHost {
			t.Fatalf("Expected host %s for dequeue %d, got %s", expectedHost, i, dequeued.URL.Host)
		}
	}
}
