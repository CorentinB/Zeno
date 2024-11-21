package index

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type serializableIndex struct {
	Index        map[string][]serializableBlob
	OrderedHosts []string
}

type serializableBlob struct {
	ID       string
	Position uint64
	Size     uint64
}

func (i *Index) GobEncode() ([]byte, error) {
	i.Lock()
	defer i.Unlock()

	si := serializableIndex{
		Index:        make(map[string][]serializableBlob),
		OrderedHosts: i.orderedHosts,
	}

	for host, blobs := range i.index {
		serializableBlobs := make([]serializableBlob, len(blobs))
		for j, blob := range blobs {
			serializableBlobs[j] = serializableBlob{
				ID:       blob.id,
				Position: blob.position,
				Size:     blob.size,
			}
		}
		si.Index[host] = serializableBlobs
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(si)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (i *Index) GobDecode(data []byte) error {
	i.Lock()
	defer i.Unlock()

	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)

	var si serializableIndex
	err := dec.Decode(&si)
	if err != nil {
		return fmt.Errorf("failed to decode index: %w", err)
	}

	i.index = make(map[string][]*blob)
	for host, serializableBlobs := range si.Index {
		blobs := make([]*blob, len(serializableBlobs))
		for j, sb := range serializableBlobs {
			blobs[j] = &blob{
				id:       sb.ID,
				position: sb.Position,
				size:     sb.Size,
			}
		}
		i.index[host] = blobs
	}
	i.orderedHosts = si.OrderedHosts

	return nil
}
