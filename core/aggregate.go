package core

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/eventsourcings/aggregated/store"
	"sync"
)

type Aggregate struct {
	Name       string
	Store      badger.DB
	EventStore store.Blocks
	Consumers  sync.Map
}
