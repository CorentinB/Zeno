package index

import (
	"fmt"
	"slices"
	"sync"
)

type Index struct {
	sync.Mutex
	index        map[string][]*blob
	orderedHosts []string
}

type blob struct {
	id       string
	position uint64
	size     uint64
}

// newIndex returns a new Index
func newIndex() *Index {
	return &Index{
		index:        make(map[string][]*blob),
		orderedHosts: []string{},
	}
}

// add is a routine safe method to add a new blob to the index
func (i *Index) add(host string, id string, position uint64, size uint64) error {
	i.Lock()
	defer i.Unlock()

	// error handling
	// position can be 0 (e.g.: when the queue file is empty)
	if id == "" {
		return ErrIDEmpty
	}

	if host == "" {
		return ErrHostEmpty
	}

	if size == 0 {
		return fmt.Errorf("invalid size=%d", size)
	}

	// If the host is not in the index, add it to the orderedHosts list
	if _, exists := i.index[host]; !exists {
		i.orderedHosts = append(i.orderedHosts, host)
	}

	// Add the blob to the index
	i.index[host] = append(i.index[host], &blob{
		id:       id,
		position: position,
		size:     size,
	})

	return nil
}

// pop is a routine safe method to pop a blob to the index
// It returns the position and size of the blob then removes it from the index
func (i *Index) pop(host string, blobChan chan *blob, WALChan chan bool) error {
	i.Lock()
	defer i.Unlock()

	// check if the host is in the index
	if _, exists := i.index[host]; !exists {
		blobChan <- nil // send nil to the blobChan to indicate the host is not in the index
		return ErrHostNotFound
	}

	// check if the host is empty
	if len(i.index[host]) == 0 {
		i.deleteHost(host)
		blobChan <- nil // send nil to the blobChan to indicate the host is empty
		return ErrHostEmpty
	}

	blobChan <- i.index[host][0] // send the blob to the getChan

	// wait for the WAL to finish writing the pop operation
	ok := <-WALChan
	if !ok {
		return nil
	}

	i.index[host] = i.index[host][1:] // remove the blob from the index

	// if the host is empty due to last item popped, remove it from the index and orderedHosts list
	if len(i.index[host]) == 0 {
		i.deleteHost(host)
	}

	return nil
}

// removeBlob is a routine safe method to remove a blob from the index based on the host and id
func (i *Index) removeBlob(host string, id string) error {
	i.Lock()
	defer i.Unlock()

	// check if the host is in the index
	if _, exists := i.index[host]; !exists {
		return ErrHostNotFound
	}

	// check if the host is empty
	if len(i.index[host]) == 0 {
		i.deleteHost(host)
		return ErrHostEmpty
	}

	// find the blob in the index
	blobIndex := 0
	found := false
	for j, b := range i.index[host] {
		if b.id == id {
			blobIndex = j
			found = true
			break
		}
	}

	// if the blob is not found, return an error
	if !found {
		return fmt.Errorf("blob with id %s not found", id)
	}

	// remove the blob from the index
	i.index[host] = append(i.index[host][:blobIndex], i.index[host][blobIndex+1:]...)

	// if the host is empty due to last item popped, remove it from the index and orderedHosts list
	if len(i.index[host]) == 0 {
		i.deleteHost(host)
	}

	return nil
}

func (i *Index) deleteHost(host string) {
	delete(i.index, host)
	// remove the host from the orderedHosts list
	orderedHostIndex := slices.Index(i.orderedHosts, host)
	i.orderedHosts = slices.Delete(i.orderedHosts, orderedHostIndex, orderedHostIndex+1)
}

// getOrderedHosts returns a copy of the orderedHosts list
func (i *Index) getOrderedHosts() []string {
	i.Lock()
	defer i.Unlock()

	hosts := make([]string, len(i.orderedHosts))
	copy(hosts, i.orderedHosts)

	return hosts
}
