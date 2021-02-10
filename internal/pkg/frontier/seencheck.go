package frontier

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/paulbellamy/ratecounter"
)

// Seencheck holds the Seencheck database and the seen counter
type Seencheck struct {
	SeenCount *ratecounter.Counter
	SeenDB    *badger.DB
}

// IsSeen check if the hash is in the seencheck database
func (seencheck *Seencheck) IsSeen(hash string) (found bool, value string, err error) {
	var item *badger.Item

	err = seencheck.SeenDB.View(func(txn *badger.Txn) error {
		item, err = txn.Get([]byte(hash))
		return err
	})

	if err == badger.ErrKeyNotFound {
		return false, "", nil
	}

	if err != nil {
		return false, "", err
	}

	err = item.Value(func(val []byte) error {
		valueBytes := append([]byte{}, val...)
		value = string(valueBytes)
		return nil
	})

	return found, value, nil
}

// Seen mark a hash as seen and increment the seen counter
func (seencheck *Seencheck) Seen(hash, value string) error {
	err := seencheck.SeenDB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(hash), []byte(value))
		return err
	})
	if err != nil {
		return err
	}
	seencheck.SeenCount.Incr(1)
	return nil
}
