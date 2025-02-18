package index

import (
	"encoding/gob"
	"os"
	"path"
	"strconv"
	"testing"
	"time"
)

func Test_Recovery(t *testing.T) {
	queueDir, err := os.MkdirTemp("", "index_test")
	defer os.RemoveAll(queueDir)
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	walPath := path.Join(queueDir, "/index_wal")
	indexPath := path.Join(queueDir, "/index")

	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		walFile.Close()
		t.Fatalf("failed to open index file: %v", err)
	}

	im := &IndexManager{
		hostIndex:    newIndex(),
		walFile:      walFile,
		indexFile:    indexFile,
		walEncoder:   gob.NewEncoder(walFile),
		walDecoder:   gob.NewDecoder(walFile),
		indexEncoder: gob.NewEncoder(indexFile),
		indexDecoder: gob.NewDecoder(indexFile),
		dumpTicker:   time.NewTicker(time.Duration(dumpFrequency) * time.Second),
		lastDumpTime: time.Now(),
	}

	// Add entries to the index
	for i := 0; i < 1000; i++ {
		_, err := im.Add("example.com", "id"+strconv.Itoa(i), uint64(i*200), uint64(200))
		if err != nil {
			t.Fatalf("failed to add entry to index: %v", err)
		}
	}

	im.Lock()

	// Nil all fields to simulate a closed index
	im.hostIndex = nil
	im.walEncoder = nil
	im.walDecoder = nil
	im.indexEncoder = nil
	im.indexDecoder = nil

	// Close file descriptors
	err = im.walFile.Close()
	if err != nil {
		t.Fatalf("failed to close WAL file: %v", err)
	}

	err = im.indexFile.Close()
	if err != nil {
		t.Fatalf("failed to close index file: %v", err)
	}

	walFile = nil
	indexFile = nil

	im.Unlock()

	im = nil

	walFile, err = os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}

	indexFile, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		walFile.Close()
		t.Fatalf("failed to open index file: %v", err)
	}

	im = &IndexManager{
		hostIndex:    newIndex(),
		walFile:      walFile,
		indexFile:    indexFile,
		walEncoder:   gob.NewEncoder(walFile),
		walDecoder:   gob.NewDecoder(walFile),
		indexEncoder: gob.NewEncoder(indexFile),
		indexDecoder: gob.NewDecoder(indexFile),
		dumpTicker:   time.NewTicker(time.Duration(dumpFrequency) * time.Second),
		lastDumpTime: time.Now(),
	}

	err = im.RecoverFromCrash()
	if err != nil {
		t.Fatalf("failed to recover from crash: %v", err)
	}
}

// Test_RecoveryAfterOneIndexDumpAndWALNotEmpty tests the recovery process after one index dump and the WAL is not empty.
// The test adds 50 entries to the index, perform a disk dump, adds 50 more entries, simulates a crash, and then recovers the index.
// This particular case was found when troubleshooting another bug and is included as a test case to prevent regressions.
func Test_RecoveryAfterOneIndexDumpAndWALNotEmpty(t *testing.T) {
	queueDir, err := os.MkdirTemp("", "index_test")
	defer os.RemoveAll(queueDir)
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}

	walPath := path.Join(queueDir, "/index_wal")
	indexPath := path.Join(queueDir, "/index")

	walFile, err := os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}

	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		walFile.Close()
		t.Fatalf("failed to open index file: %v", err)
	}

	im := &IndexManager{
		hostIndex:    newIndex(),
		walFile:      walFile,
		indexFile:    indexFile,
		walEncoder:   gob.NewEncoder(walFile),
		walDecoder:   gob.NewDecoder(walFile),
		indexEncoder: gob.NewEncoder(indexFile),
		indexDecoder: gob.NewDecoder(indexFile),
		dumpTicker:   time.NewTicker(time.Duration(dumpFrequency) * time.Second),
		lastDumpTime: time.Now(),
	}

	// Add entries to the index
	for i := 0; i < 50; i++ {
		_, err := im.Add("example.com", "id"+strconv.Itoa(i), uint64(i*200), uint64(200))
		if err != nil {
			t.Fatalf("failed to add entry to index: %v", err)
		}
	}

	// Perform a disk dump
	err = im.performDump()
	if err != nil {
		t.Fatalf("failed to dump index: %v", err)
	}

	// Add more entries to the index
	for i := 50; i < 100; i++ {
		_, err := im.Add("example.com", "id"+strconv.Itoa(i), uint64(i*200), uint64(200))
		if err != nil {
			t.Fatalf("failed to add entry to index: %v", err)
		}
	}

	im.Lock()

	// Nil all fields to simulate a closed index
	im.hostIndex = nil
	im.walEncoder = nil
	im.walDecoder = nil
	im.indexEncoder = nil
	im.indexDecoder = nil

	// Close file descriptors
	err = im.walFile.Close()
	if err != nil {
		t.Fatalf("failed to close WAL file: %v", err)
	}

	err = im.indexFile.Close()
	if err != nil {
		t.Fatalf("failed to close index file: %v", err)
	}

	walFile = nil
	indexFile = nil

	im.Unlock()

	im = nil

	walFile, err = os.OpenFile(walPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open WAL file: %v", err)
	}

	indexFile, err = os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		walFile.Close()
		t.Fatalf("failed to open index file: %v", err)
	}

	im = &IndexManager{
		hostIndex:    newIndex(),
		walFile:      walFile,
		indexFile:    indexFile,
		walEncoder:   gob.NewEncoder(walFile),
		walDecoder:   gob.NewDecoder(walFile),
		indexEncoder: gob.NewEncoder(indexFile),
		indexDecoder: gob.NewDecoder(indexFile),
		dumpTicker:   time.NewTicker(time.Duration(dumpFrequency) * time.Second),
		lastDumpTime: time.Now(),
	}

	err = im.RecoverFromCrash()
	if err != nil {
		t.Fatalf("failed to recover from crash: %v", err)
	}
}
