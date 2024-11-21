package models

import (
	"github.com/google/uuid"
)

// Item represents a URL, it's childs (e.g. discovered assets) and it's state in the pipeline
type Item struct {
	ID             string     // ID is the unique identifier of the item
	URL            *URL       // URL is a struct that contains the URL, the parsed URL, and its hop
	Status         ItemState  // Status is the state of the item in the pipeline
	Source         ItemSource // Source is the source of the item in the pipeline
	ChildsCaptured bool       // ChildsCaptured is the flag to indicate if the child URLs of the item have been captured
	Redirection    *URL       // Redirection is the URL that the item has been redirected to, if it's not nil it need to be captured
	Via            string     // Via is the URL that the item has been found from
	Childs         []*URL     // Childs is the list of URLs that have been discovered via the item's URL
	Error          error      // Error message of the seed
}

func NewItem(source ItemSource) (item *Item) {
	UUID := uuid.New().String()

	item = &Item{
		ID:     UUID,
		Status: ItemFresh,
		Source: source,
	}

	return item
}

func (i *Item) AddChild(child *URL) {
	i.Childs = append(i.Childs, child)
}

func (i *Item) GetChilds() []*URL {
	return i.Childs
}

func (i *Item) GetID() string {
	return i.ID
}

func (i *Item) GetShortID() string {
	return i.ID[:5]
}

func (i *Item) GetURL() *URL {
	return i.URL
}

func (i *Item) GetStatus() ItemState {
	return i.Status
}

func (i *Item) GetSource() ItemSource {
	return i.Source
}

func (i *Item) GetChildsCaptured() bool {
	return i.ChildsCaptured
}

func (i *Item) GetRedirection() *URL {
	return i.Redirection
}

func (i *Item) GetError() error {
	return i.Error
}

func (i *Item) SetURL(url *URL) {
	i.URL = url
}

func (i *Item) SetStatus(status ItemState) {
	i.Status = status
}

func (i *Item) SetSource(source ItemSource) {
	i.Source = source
}

func (i *Item) SetChilds(childs []*URL) {
	i.Childs = childs
}

func (i *Item) SetChildsCaptured(captured bool) {
	i.ChildsCaptured = captured
}

func (i *Item) SetRedirection(redirection *URL) {
	i.Redirection = redirection
}

func (i *Item) SetError(err error) {
	i.Error = err
}

// ItemState qualifies the state of a item in the pipeline
type ItemState int

const (
	// ItemFresh is the initial state of a item either it's from HQ, the Queue or Feedback
	ItemFresh ItemState = iota
	// ItemPreProcessed is the state after the item has been pre-processed
	ItemPreProcessed
	// ItemCaptured is the state after the item has been captured
	ItemCaptured
	// ItemPostProcessed is the state after the item has been post-processed
	ItemPostProcessed
	// ItemFailed is the state after the item has failed
	ItemFailed
	// ItemCompleted is the state after the item has been completed
	ItemCompleted
)

// ItemSource qualifies the source of a item in the pipeline
type ItemSource int

const (
	// ItemSourceInsert is for items which source is not defined when inserted on reactor
	ItemSourceInsert ItemSource = iota
	// ItemSourceQueue is for items that are from the Queue
	ItemSourceQueue
	// ItemSourceHQ is for items that are from the HQ
	ItemSourceHQ
	// ItemSourcePostprocess is for items generated from redirections
	ItemSourcePostprocess
	// ItemSourceFeedback is for items that are from the Feedback
	ItemSourceFeedback
)
