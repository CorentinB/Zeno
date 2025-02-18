package index

import (
	"os"
	"testing"
)

func Test_isWALEmpty(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	isEmpty, err := im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if !isEmpty {
		t.Fatal("expected WAL to be empty")
	}

	// Write to WAL
	err = im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 0)
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	isEmpty, err = im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if isEmpty {
		t.Fatal("expected WAL not to be empty")
	}
}

func Test_writeToWAL_Then_replayWAL(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	var replayedEntries int

	// Write to WAL
	err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200)
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	// Replay WAL
	err = im.unsafeReplayWAL(&replayedEntries)
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != 1 {
		t.Fatalf("expected 1 entry to be replayed, got: %d", replayedEntries)
	}
}

func Test_bigreplayWAL(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	numEntries := 1000

	// Write to WAL
	for i := 0; i < numEntries; i++ {
		err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200)
		if err != nil {
			t.Fatalf("failed to write to WAL: %v", err)
		}
	}
	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	// Replay WAL
	var replayedEntries int
	err := im.unsafeReplayWAL(&replayedEntries)
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != numEntries {
		t.Fatalf("expected %d entries to be replayed, got: %d", numEntries, replayedEntries)
	}
}

func Test_writeToWAL_Then_truncateWAL(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	// Write to WAL
	err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200)
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	// Truncate WAL
	err = im.unsafeTruncateWAL()
	if err != nil {
		t.Fatalf("failed to truncate WAL: %v", err)
	}

	// Check if WAL is empty
	isEmpty, err := im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if !isEmpty {
		t.Fatal("expected WAL to be empty after truncation")
	}
}

// Test_WAL_combined tests the combined functionality of writing, replaying, and truncating the WAL.
// It writes a number of entries to the WAL, replays it, truncates it, replays it, writes more entries, replays it again.
func Test_WAL_combined(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	numEntries := 1000

	// Write to WAL
	for i := 0; i < numEntries; i++ {
		err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200+uint64(i))
		if err != nil {
			t.Fatalf("failed to write to WAL: %v", err)
		}
	}

	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	// Replay WAL
	var replayedEntries int
	err := im.unsafeReplayWAL(&replayedEntries)
	if err != nil && err != ErrNoWALEntriesReplayed {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != numEntries {
		t.Fatalf("expected 0 entries to be replayed, got: %d", replayedEntries)
	}

	replayedEntries = 0

	// Truncate WAL
	err = im.unsafeTruncateWAL()
	if err != nil {
		t.Fatalf("failed to truncate WAL: %v", err)
	}

	// Check if WAL is empty
	isEmpty, err := im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if !isEmpty {
		t.Fatal("expected WAL to be empty after truncation")
	}

	// Replay WAL
	err = im.unsafeReplayWAL(&replayedEntries)
	if err != nil && err != ErrNoWALEntriesReplayed {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != 0 {
		t.Fatalf("expected 0 entries to be replayed, got: %d", replayedEntries)
	}

	replayedEntries = 0

	// Write to WAL again
	for i := 0; i < numEntries; i++ {
		err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200+uint64(i))
		if err != nil {
			t.Fatalf("failed to write to WAL: %v", err)
		}
	}

	// Check if WAL is empty
	isEmpty, err = im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if isEmpty {
		t.Fatal("expected WAL to be non-empty after writing")
	}

	// Replay WAL
	err = im.unsafeReplayWAL(&replayedEntries)
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != numEntries {
		t.Fatalf("expected %d entries to be replayed, got: %d", numEntries, replayedEntries)
	}

	// Check if WAL is empty
	isEmpty, err = im.unsafeIsWALEmpty()
	if err != nil {
		t.Fatalf("failed to check if WAL is empty: %v", err)
	}
	if isEmpty {
		t.Fatal("expected WAL to be non-empty after replaying")
	}
}

func Test_WAL_WriteAfterNonZeroReplay(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	numEntries := 1000

	// Write to WAL
	for i := 0; i < numEntries; i++ {
		err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200+uint64(i))
		if err != nil {
			t.Fatalf("failed to write to WAL: %v", err)
		}
	}

	// Replay WAL
	var replayedEntries int
	err := im.unsafeReplayWAL(&replayedEntries)
	if err != nil {
		t.Fatalf("failed to replay WAL: %v", err)
	}

	if replayedEntries != numEntries {
		t.Fatalf("expected %d entries to be replayed, got: %d", numEntries, replayedEntries)
	}

	// Write to WAL again
	err = im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200)
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	// Replay WAL
	replayedEntries = 0
	err = im.unsafeReplayWAL(&replayedEntries)
	if err == nil {
		t.Fatalf("expected to fail replaying WAL after writing")
	}
}

func Test_replayWAL_error(t *testing.T) {
	im, tempDir := provideTestIndexManager(t, false)
	defer os.RemoveAll(tempDir)
	im.Lock()
	defer im.Unlock()

	// Write to WAL
	err := im.unsafeWriteToWAL(OpAdd, "example.com", "id", 0, 200)
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	if err := im.unsafeWalSync(); err != nil {
		t.Fatalf("failed to sync WAL: %v", err)
	}

	// Corrupt the WAL
	_, err = im.walFile.Write([]byte("corruption"))
	if err != nil {
		t.Fatalf("failed to write to WAL: %v", err)
	}

	// Replay WAL
	var replayedEntries int
	err = im.unsafeReplayWAL(&replayedEntries)
	if err == nil {
		t.Fatal("expected replayWAL to fail")
	}
}
