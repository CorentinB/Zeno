package frontier

import (
	"strconv"
	"time"

	"github.com/paulbellamy/ratecounter"
	"github.com/sirupsen/logrus"
)

func (f *Frontier) writeItemsToQueue() {
	f.IsQueueWriterActive.Set(true)

	for item := range f.PushChan {
		item := item

		if f.Paused.Get() {
			time.Sleep(time.Second)
		}

		// If --seencheck is enabled, then we check if the URI is in the
		// seencheck DB before doing anything. If it is in it, we skip the item
		if f.UseSeencheck {
			hash := strconv.FormatUint(item.Hash, 10)
			found, value := f.Seencheck.IsSeen(hash)
			if !found || (value == "asset" && item.Type == "seed") {
				f.Seencheck.Seen(hash, item.Type)
			} else {
				continue
			}
		}

		// Increment the counter of the host in the hosts pool,
		// if the hosts doesn't exist in the pool, it will be created
		f.HostPool.Incr(item.Host)

		// Add the item to the host's queue
		_, err := f.Queue.EnqueueObject([]byte(item.Host), item)
		if err != nil {
			logWarning.WithFields(logrus.Fields{
				"error": err,
				"item":  item,
			}).Error("Unable to enqueue item")
		}
		f.QueueCount.Incr(1)

		logInfo.WithFields(logrus.Fields{
			"url": item.URL,
		}).Debug("Item enqueued")
	}

	if f.FinishingQueueWriter.Get() == true {
		f.IsQueueWriterActive.Set(false)
		return
	}
}

func (f *Frontier) readItemsFromQueue() {
	var mapCopy map[string]*ratecounter.Counter

	f.IsQueueReaderActive.Set(true)

	if f.QueueCount.Value() == 0 {
		time.Sleep(time.Second)
	}

	for {
		if f.Paused.Get() {
			time.Sleep(time.Second)
		}

		// We cleanup the hosts pool by removing
		// all the hosts with a count of 0, then
		// we make a snapshot of the hosts
		// pool that we will iterate on
		f.HostPool.DeleteEmptyHosts()
		mapCopy = make(map[string]*ratecounter.Counter, 0)
		f.HostPool.Lock()
		for key, val := range f.HostPool.Hosts {
			mapCopy[key] = val
		}
		f.HostPool.Unlock()

		// We iterate over the copied pool, and dequeue
		// new URLs to crawl based on that hosts pool
		// that allow us to crawl a wide variety of domains
		// at the same time, maximizing our speed
		for host := range mapCopy {
			if f.Paused.Get() {
				time.Sleep(time.Second)
			}

			if f.HostPool.GetCount(host) == 0 {
				continue
			}

			// Dequeue an item from the local queue
			queueItem, err := f.Queue.DequeueString(host)
			if err != nil {
				logWarning.WithFields(logrus.Fields{
					"error": err,
				}).Debug("Unable to dequeue item")
				if err.Error() == "goque: Stack or queue is empty" ||
					err.Error() == "goque: ID used is outside range of stack or queue" {
					f.HostPool.Decr(host)
				}
				continue
			}
			f.QueueCount.Incr(-1)

			// Turn the item from the queue into an Item
			var item *Item
			err = queueItem.ToObject(&item)
			if err != nil {
				logWarning.WithFields(logrus.Fields{
					"error": err,
				}).Error("Unable to parse queue's item")
				continue
			}

			// Sending the item to the workers via PullChan
			f.PullChan <- item
			logInfo.WithFields(logrus.Fields{
				"url": item.URL,
			}).Debug("Item sent to workers pool")

			f.HostPool.Decr(host)

			if f.FinishingQueueReader.Get() == true {
				f.IsQueueReaderActive.Set(false)
				return
			}
		}
	}
}
