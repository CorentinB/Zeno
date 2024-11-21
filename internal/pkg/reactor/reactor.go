// Package reactor provides functionality to manage and control the processing of seeds.
package reactor

import (
	"context"
	"sync"

	"github.com/internetarchive/Zeno/internal/pkg/log"
	"github.com/internetarchive/Zeno/pkg/models"
)

// reactor struct holds the state and channels for managing seeds processing.
type reactor struct {
	tokenPool    chan struct{}      // Token pool to control asset count
	ctx          context.Context    // Context for stopping the reactor
	cancel       context.CancelFunc // Context's cancel func
	freezeCtx    context.Context    // Context for freezing the reactor
	freezeCancel context.CancelFunc // Freezing context's cancel func
	input        chan *models.Item  // Combined input channel for source and feedback
	output       chan *models.Item  // Output channel
	stateTable   sync.Map           // State table for tracking seeds by UUID
	wg           sync.WaitGroup     // WaitGroup to manage goroutines
	// stopChan   chan struct{}      // Channel to signal when stop is finished
}

var (
	globalReactor *reactor
	once          sync.Once
	logger        *log.FieldedLogger
)

// Start initializes the global reactor with the given maximum tokens.
// This method can only be called once.
func Start(maxTokens int, outputChan chan *models.Item) error {
	var done bool

	log.Start()
	logger = log.NewFieldedLogger(&log.Fields{
		"component": "reactor",
	})

	once.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		freezeCtx, freezeCancel := context.WithCancel(ctx)
		globalReactor = &reactor{
			tokenPool:    make(chan struct{}, maxTokens),
			ctx:          ctx,
			cancel:       cancel,
			freezeCtx:    freezeCtx,
			freezeCancel: freezeCancel,
			input:        make(chan *models.Item, maxTokens),
			output:       outputChan,
		}
		logger.Debug("initialized")
		globalReactor.wg.Add(1)
		go globalReactor.run()
		logger.Info("started")
		done = true
	})

	if !done {
		return ErrReactorAlreadyInitialized
	}

	return nil
}

// Stop stops the global reactor and waits for all goroutines to finish.
func Stop() {
	if globalReactor != nil {
		logger.Debug("received stop signal")
		globalReactor.cancel()
		globalReactor.wg.Wait()
		close(globalReactor.input)
		once = sync.Once{}
		globalReactor = nil
		logger.Info("stopped")
	}
}

// Freeze stops the global reactor from processing seeds.
func Freeze() {
	if globalReactor != nil {
		logger.Debug("received freeze signal")
		globalReactor.freezeCancel()
	}
}

// ReceiveFeedback sends an item to the feedback channel.
// If the item is not present on the state table it gets discarded
func ReceiveFeedback(item *models.Item) error {
	if globalReactor == nil {
		return ErrReactorNotInitialized
	}

	item.Source = models.ItemSourceFeedback
	_, loaded := globalReactor.stateTable.Swap(item.ID, item)
	if !loaded {
		// An item sent to the feedback channel should be present on the state table, if not present reactor should error out
		return ErrFeedbackItemNotPresent
	}
	select {
	case <-globalReactor.ctx.Done():
		return ErrReactorShuttingDown
	case <-globalReactor.freezeCtx.Done():
		return ErrReactorFrozen
	case globalReactor.input <- item:
		return nil
	}
}

// ReceiveInsert sends an item to the input channel consuming a token.
// It is the responsibility of the sender to set either ItemSourceQueue or ItemSourceHQ, if not set seed will get forced ItemSourceInsert
func ReceiveInsert(item *models.Item) error {
	logger.Debug("received item", "item", item.GetShortID())
	if globalReactor == nil {
		return ErrReactorNotInitialized
	}

	select {
	case <-globalReactor.ctx.Done():
		return ErrReactorShuttingDown
	case <-globalReactor.freezeCtx.Done():
		return ErrReactorFrozen
	case globalReactor.tokenPool <- struct{}{}:
		if item.Source != models.ItemSourceQueue && item.Source != models.ItemSourceHQ {
			item.Source = models.ItemSourceInsert
		}
		globalReactor.stateTable.Store(item.ID, item)
		globalReactor.input <- item
		return nil
	}
}

// MarkAsFinished marks an item as finished and releases a token if found in the state table.
func MarkAsFinished(item *models.Item) error {
	if globalReactor == nil {
		return ErrReactorNotInitialized
	}

	if _, loaded := globalReactor.stateTable.LoadAndDelete(item.ID); loaded {
		<-globalReactor.tokenPool
		return nil
	}
	return ErrFinisehdItemNotFound
}

func (r *reactor) run() {
	defer r.wg.Done()

	for {
		select {
		// Closes the run routine when context is canceled
		case <-r.ctx.Done():
			logger.Debug("shutting down")
			return

		// Feeds items to the output channel
		case item, ok := <-r.input:
			if ok {
				r.output <- item
			}
		}
	}
}
