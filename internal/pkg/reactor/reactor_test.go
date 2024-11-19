package reactor

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/internetarchive/Zeno/pkg/models"
	"github.com/internetarchive/gocrawlhq"
)

func TestReactorE2E(t *testing.T) {
	// Initialize the reactor with a maximum of 5 tokens
	outputChan := make(chan *models.Item)
	err := Start(1, outputChan)
	if err != nil {
		t.Logf("Error starting reactor: %s", err)
		return
	}
	defer Stop()

	// Consume items from the output channel, start 5 goroutines
	for i := 0; i < 5; i++ {
		go func(t *testing.T) {
			for {
				select {
				case item := <-outputChan:
					if item == nil {
						continue
					}
					// Send feedback for the consumed item
					if item.Source != models.ItemSourceFeedback {
						err := ReceiveFeedback(item)
						if err != nil {
							t.Fatalf("Error sending feedback: %s - %s", err, item.UUID.String())
						}
						continue
					}

					// Mark the item as finished
					if item.Source == models.ItemSourceFeedback {
						err := MarkAsFinished(item)
						if err != nil {
							t.Fatalf("Error marking item as finished: %s", err)
						}
						continue
					}
				}
			}
		}(t)
	}

	// Create mock seeds
	mockItems := []*models.Item{}
	for i := 0; i <= 1000; i++ {
		uuid := uuid.New()
		mockItems = append(mockItems, &models.Item{
			UUID:   &uuid,
			URL:    &gocrawlhq.URL{Value: fmt.Sprintf("http://example.com/%d", i)},
			Status: models.ItemFresh,
			Source: models.ItemSourceHQ,
		})
	}

	// Queue mock seeds to the source channel
	for _, seed := range mockItems {
		err := ReceiveInsert(seed)
		if err != nil {
			t.Fatalf("Error queuing seed to source channel: %s", err)
		}
	}

	// Allow some time for processing
	time.Sleep(5 * time.Second)
	if len(GetStateTable()) > 0 {
		t.Fatalf("State table is not empty: %s", GetStateTable())
	}
}
