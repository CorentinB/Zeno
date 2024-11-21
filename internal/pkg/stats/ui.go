package stats

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rivo/tview"
)

var (
	ctx    context.Context
	cancel context.CancelFunc
)

func StartUI() {
	// Create a context to manage goroutines
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup

	wg.Add(1)
	go ui(ctx, &wg)

	// Wait for the context to be canceled.
	for {
		select {
		case <-ctx.Done():
			cancel()
			wg.Wait()
			return
		}
	}
}

func StopUI() {
	cancel()
}

func ui(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	// Create a new application
	app := tview.NewApplication()

	// Create text views for each stat
	urlsCrawledText := tview.NewTextView().SetDynamicColors(true)
	seedsFinishedText := tview.NewTextView().SetDynamicColors(true)
	preprocessorRoutinesText := tview.NewTextView().SetDynamicColors(true)
	archiverRoutinesText := tview.NewTextView().SetDynamicColors(true)
	postprocessorRoutinesText := tview.NewTextView().SetDynamicColors(true)

	// Create a flex layout to hold the text views
	flex := tview.NewFlex().
		SetDirection(tview.FlexRow).
		AddItem(urlsCrawledText, 0, 1, false).
		AddItem(seedsFinishedText, 0, 1, false).
		AddItem(preprocessorRoutinesText, 0, 1, false).
		AddItem(archiverRoutinesText, 0, 1, false).
		AddItem(postprocessorRoutinesText, 0, 1, false)

	// Function to update the stats
	go func(ctx context.Context) {
		for {
			select {
			case <-ctx.Done():
				app.Stop()
				return
			default:
				// Sleep for a while before updating again
				time.Sleep(250 * time.Millisecond)

				// Update text views
				urlsCrawledText.SetText(fmt.Sprintf("URLs Crawled: %d", URLsCrawledGet()))
				seedsFinishedText.SetText(fmt.Sprintf("Seeds Finished: %d", SeedsFinishedGet()))
				preprocessorRoutinesText.SetText(fmt.Sprintf("Preprocessor Routines: %d", PreprocessorRoutinesGet()))
				archiverRoutinesText.SetText(fmt.Sprintf("Archiver Routines: %d", ArchiverRoutinesGet()))
				postprocessorRoutinesText.SetText(fmt.Sprintf("Postprocessor Routines: %d", PostprocessorRoutinesGet()))

				// Refresh the UI
				app.Draw()
			}
		}
	}(ctx)

	if err := app.SetRoot(flex, true).Run(); err != nil {
		panic(err)
	}
}
