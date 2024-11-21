# Preprocessor Package Documentation
## Overview
The preprocessor package provides functionality to prepare seeds for capture. It includes mechanisms for validating URLs and preprocessoring items before they are sent for capture. The package ensures that operations are atomic and synchronized, maintaining consistency and avoiding race conditions.

The preprocessor package is designed to be used in a concurrent environment, where multiple goroutines may interact with the preprocessor. It uses channels to manage the flow of items and their preprocessoring status. The package is thread-safe and provides error handling for common scenarios.

## Installation
To use the preprocessor package, import it into your package:
```go
import "github.com/internetarchive/Zeno/internal/pkg/preprocessor"
```

## Usage
### Initialization
Before using the preprocessor, you need to initialize it with input and output channels:
```go
inputChan := make(chan *models.Item)
outputChan := make(chan *models.Item)
err := preprocessor.Start(inputChan, outputChan)
if err != nil {
    log.Fatalf("Error starting preprocessor: %v", err)
}
defer preprocessor.Stop()
```
The initialization should happen once or it will error out with
```
ErrPreprocessorAlreadyInitialized || ErrPreprocessorNotInitialized
```

### Preprocessoring Items
To preprocessor an item, send it to the input channel:
```go
item := &models.Item{
    UUID:   uuid.New(),
    URL:    &gocrawlhq.URL{Value: "http://example.com"},
    Status: models.ItemFresh,
}
inputChan <- item
```
The preprocessored item will be sent to the output channel after preprocessoring.

## Internals
### Preprocessor Struct
The preprocessor struct holds the state and channels for managing item preprocessoring:
```go
type preprocessor struct {
    wg     sync.WaitGroup
    ctx    context.Context
    cancel context.CancelFunc
    input  chan *models.Item
    output chan *models.Item
}
```