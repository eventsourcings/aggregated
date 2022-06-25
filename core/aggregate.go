package core

import "github.com/eventsourcings/aggregated/store"

type Aggregate struct {
	Name   string
	Events store.Blocks
}
