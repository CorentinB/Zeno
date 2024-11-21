package index

import (
	"fmt"
	"os"
	"path/filepath"
)

func (im *IndexManager) RecoverFromCrash() error {
	im.Lock()
	defer im.Unlock()

	im.logger.Warn("starting crash recovery process")

	// Step 1: Load the index file into the in-memory index
	if err := im.loadIndex(); err != nil {
		return fmt.Errorf("failed to load index during recovery: %w", err)
	}

	// Step 2: Replay the WAL
	var replayedEntries int
	if err := im.unsafeReplayWAL(&replayedEntries); err != nil && err != ErrNoWALEntriesReplayed {
		return fmt.Errorf("failed to replay WAL during recovery: %w", err)
	}

	// Step 3: Perform a new index dump
	tempFile, err := os.CreateTemp(filepath.Dir(im.indexFile.Name()), "index_recovery_")
	if err != nil {
		return fmt.Errorf("failed to create temp file for recovery dump: %w", err)
	}
	defer tempFile.Close()

	if err := im.dumpIndexToFile(tempFile); err != nil {
		return fmt.Errorf("failed to dump index during recovery: %w", err)
	}

	// Step 4: Rename files
	oldIndexPath := im.indexFile.Name() + ".old"
	if err := os.Rename(im.indexFile.Name(), oldIndexPath); err != nil {
		return fmt.Errorf("failed to rename current index file: %w", err)
	}

	if err := os.Rename(tempFile.Name(), im.indexFile.Name()); err != nil {
		// Try to rollback if rename fails
		os.Rename(oldIndexPath, im.indexFile.Name())
		return fmt.Errorf("failed to rename new index file: %w", err)
	}

	// Step 5: Remove old index file
	if err := os.Remove(oldIndexPath); err != nil {
		im.logger.Warn("failed to remove old index file", "error", err)
	}

	// Step 6: Truncate and reset WAL
	if err := im.unsafeTruncateWAL(); err != nil {
		return fmt.Errorf("failed to truncate WAL during recovery: %w", err)
	}

	return nil
}
