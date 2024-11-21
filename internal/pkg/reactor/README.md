# Reactor Package Documentation
## Overview
The reactor package provides functionality to manage and control the processing of seeds. It includes mechanisms for inserting seeds, receiving feedback, and marking seeds as finished. The package ensures that operations are atomic and synchronized, maintaining consistency and avoiding race conditions.  

The reactor package is designed to be used in a concurrent environment, where multiple goroutines may interact with the reactor. It uses channels and a state table to manage the flow of seeds and their processing status. The package is thread-safe and provides error handling for common scenarios.  

A token-based system is used to limit the number of seeds processed concurrently. The reactor can be initialized with a maximum number of tokens, which determines the number of seeds that can be processed simultaneously. This helps prevent overloading the system and ensures efficient resource utilization.

## Installation
To use the reactor package, import it into your package:
```go
import "github.com/internetarchive/Zeno/pkg/reactor"
```

## Usage
### Initialization
Before using the reactor, you need to initialize it with a maximum number of tokens and an output channel:
```go
outputChan := make(chan *models.Seed)
err := reactor.Start(5, outputChan)
if err != nil {
    log.Fatalf("Error starting reactor: %v", err)
}
defer reactor.Stop()
```
The initialization should happen once or it will error out with
```
ErrErrReactorAlreadyInitialized || ErrReactorNotInitialized
```

### Inserting Seeds
To insert a seed into the reactor, use the ReceiveInsert function:
```go
seed := &models.Seed{
    UUID:   uuid.New(),
    URL:    &gocrawlhq.URL{Value: "http://example.com"},
    Status: models.SeedFresh,
    Source: models.SeedSourceHQ,
}

err := reactor.ReceiveInsert(seed)
if err != nil {
    log.Fatalf("Error inserting seed: %v", err)
}
```
Inserting a seed will consume a token if available, allowing the seed to be processed. If no tokens are available, the function will block until a token is released and the seed can be inserted into the reactor.

### Feedback a Seed
To send a seed for feedback, use the ReceiveFeedback function:
```go
err := reactor.ReceiveFeedback(seed)
if err != nil {
    log.Fatalf("Error sending feedback: %v", err)
}
```
Feedback can be used to reprocess a seed that got assets added. The seed will be reinserted into the reactor without consuming a token, cause it already consumed a token when inserted.

### Marking Seeds as Finished
To mark a seed as finished, use the MarkAsFinished function:
```go
err := reactor.MarkAsFinished(seed)
if err != nil {
    log.Fatalf("Error marking seed as finished: %v", err)
}
```
Marking a seed as finished will release a token if the seed was inserted first. If the seed was not inserted, the function will error out with :
```go
ErrFinisehdItemNotFound
```

## Internals
### Reactor Struct
The reactor struct holds the state and channels for managing seed processing:
```go
type reactor struct {
	tokenPool  chan struct{}      // Token pool to control asset count
	ctx        context.Context    // Context for stopping the reactor
	cancel     context.CancelFunc // Context's cancel func
	input      chan *models.Seed  // Combined input channel for source and feedback
	output     chan *models.Seed  // Output channel
	stateTable sync.Map           // State table for tracking seeds by UUID
	wg         sync.WaitGroup     // WaitGroup to manage goroutines
}
```

### Maintaining Equilibrium
The reactor maintains equilibrium in the system through the following mechanisms:

1. Token-Based Concurrency Control:
    - The token pool limits the number of concurrent seeds being processed.
    - Each seed consumes a token when inserted and releases it when marked as finished.
    - This prevents overloading the system and ensures efficient resource utilization.
2. Channel Operations:
    - The reactor uses a channel-based synchronization mechanism with a buffer on the input channel ensuring that no deadlock can happen.
    - The output channel is expected to be unbuffered.
3. State Management:
    - The state table tracks the state of each seed by its UUID.
    - The state map is held to check that every seed that goes into feedback was already ingested first, ensuring a fixed amount of seeds in the system.
    - This allows the reactor to manage seeds efficiently and handle feedback and completion correctly.

## Conclusion
The `reactor` package provides a robust and synchronized mechanism for managing seed processing in a concurrent environment. By using channels, a state table, and a token-based system, it ensures efficient resource utilization and maintains equilibrium in the system. This architecture allows for scalable and reliable seed processing without sacrificing efficiency