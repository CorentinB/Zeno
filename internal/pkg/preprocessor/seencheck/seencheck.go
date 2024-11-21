package seencheck

import (
	"hash/fnv"
	"path"
	"strconv"
	"sync/atomic"

	"github.com/internetarchive/Zeno/pkg/models"
	"github.com/philippgille/gokv/leveldb"
)

// Seencheck holds the Seencheck database and the seen counter
type Seencheck struct {
	Count *int64
	DB    leveldb.Store
}

var (
	globalSeencheck *Seencheck
)

func Start(jobPath string) (err error) {
	count := int64(0)
	globalSeencheck = new(Seencheck)
	globalSeencheck.Count = &count
	globalSeencheck.DB, err = leveldb.NewStore(leveldb.Options{Path: path.Join(jobPath, "seencheck")})
	return err
}

func Close() {
	globalSeencheck.DB.Close()
}

func isSeen(hash string) (found bool, value string) {
	found, err := globalSeencheck.DB.Get(hash, &value)
	if err != nil {
		panic(err)
	}

	return found, value
}

func seen(hash, value string) {
	globalSeencheck.DB.Set(hash, value)
	atomic.AddInt64(globalSeencheck.Count, 1)
}

func SeencheckURLs(URLType string, URLs ...*models.URL) (seencheckedURLs []*models.URL, err error) {
	h := fnv.New64a()

	for _, URL := range URLs {
		_, err = h.Write([]byte(URL.String()))
		if err != nil {
			return nil, err
		}

		hash := strconv.FormatUint(h.Sum64(), 10)

		found, _ := isSeen(hash)
		if !found {
			seen(hash, URLType)
			seencheckedURLs = append(seencheckedURLs, URL)
		}
	}

	return seencheckedURLs, nil
}
