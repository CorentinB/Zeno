package queue

import (
	"os"
	"path"
	"testing"
)

func Test_canEnqueue(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	q, err := NewPersistentGroupedQueue(path.Join(tempDir, "test_queue"))
	if err != nil {
		t.Fatalf("Failed to create new queue: %v", err)
	}
	defer q.Close()

	if !q.CanEnqueue() {
		t.Fatalf("Expected canEnqueue to return true, got false")
	}
}

func Test_canDequeue(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "queue_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	q, err := NewPersistentGroupedQueue(path.Join(tempDir, "test_queue"))
	if err != nil {
		t.Fatalf("Failed to create new queue: %v", err)
	}
	defer q.Close()

	if !q.CanDequeue() {
		t.Fatalf("Expected canDequeue to return true, got false")
	}
}
