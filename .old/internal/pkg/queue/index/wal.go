package index

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

// unsafeWriteToWAL writes an entry to the WAL file. This method is not thread-safe and should be called after acquiring a lock.
func (im *IndexManager) unsafeWriteToWAL(Op Operation, host, id string, position, size uint64) error {
	// Write to WAL
	entry := WALEntry{
		Op:       Op,
		Host:     host,
		BlobID:   id,
		Position: position,
		Size:     size,
	}

	if err := im.walEncoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}

	return nil
}

// unsafeReplayWAL replays the WAL file. This method is not thread-safe and should be called after acquiring a lock.
// If entriesReplayed is not nil, it will be set to the number of entries replayed.
// If the WAL file is empty, this method returns ErrNoWALEntriesReplayed.
// If more than 0 entries are replayed, you MUST call unsafeTruncateWAL after it to avoid encoding issues.
func (im *IndexManager) unsafeReplayWAL(entriesReplayed *int) error {
	_, err := im.walFile.Seek(0, 0)
	if err != nil {
		return fmt.Errorf("failed to seek to the beginning of WAL: %w", err)
	}

	var tempEntriesReplayed int
	for {
		var entry WALEntry
		if err := im.walDecoder.Decode(&entry); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("failed to decode WAL entry: %w", err)
		}

		switch entry.Op {
		case OpAdd:
			err := im.hostIndex.add(entry.Host, entry.BlobID, entry.Position, entry.Size)
			if err != nil {
				return fmt.Errorf("failed to replay WAL entry: %w", err)
			}
		case OpPop:
			err := im.hostIndex.removeBlob(entry.Host, entry.BlobID)
			if err != nil {
				return fmt.Errorf("failed to replay WAL entry: %w", err)
			}
		default:
			return fmt.Errorf("unknown WAL operation: %v", entry.Op)
		}
		tempEntriesReplayed++
	}

	if tempEntriesReplayed == 0 {
		return ErrNoWALEntriesReplayed
	}

	if entriesReplayed != nil {
		*entriesReplayed = tempEntriesReplayed
	}
	return nil
}

// unsafeTruncateWAL truncates the WAL file. This method is not thread-safe and should be called after acquiring a lock.
func (im *IndexManager) unsafeTruncateWAL() error {
	// Save WAL file path
	walPath := im.walFile.Name()

	// Void the encoder/decoder
	im.walEncoder = nil
	im.walDecoder = nil

	// Close the current WAL file
	err := im.walFile.Close()
	if err != nil {
		return fmt.Errorf("failed to close WAL file: %w", err)
	}

	// Reopen the file with O_TRUNC flag to truncate it
	walFile, err := os.OpenFile(walPath, os.O_TRUNC|walFileOpenFlags, 0644)
	if err != nil {
		return fmt.Errorf("failed to truncate WAL file: %w", err)
	}

	// Replace the old file descriptor with the new one
	im.walFile = walFile

	// Create a new encoder and decoder for the fresh WAL file
	im.walEncoder = gob.NewEncoder(im.walFile)
	im.walDecoder = gob.NewDecoder(im.walFile)

	return nil
}

// unsafeIsWALEmpty checks if the WAL file is empty. Returns true if empty.
// This method is not thread-safe and should be called after acquiring a lock.
func (im *IndexManager) unsafeIsWALEmpty() (bool, error) {
	stat, err := im.walFile.Stat()
	if err != nil {
		return false, fmt.Errorf("failed to get WAL file info: %w", err)
	}
	return stat.Size() == 0, nil
}
