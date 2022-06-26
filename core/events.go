package core

import (
	"fmt"
	"sort"
)

type Event struct {
	Id          uint64
	Aggregate   string
	AggregateId string
	Name        string
	Content     []byte
}

func (event *Event) Key() (p []byte) {
	p = []byte(fmt.Sprintf("%s:%s:%s:%d", event.Aggregate, event.AggregateId, event.Name, event.Id))
	return
}

type Events []*Event

func (events Events) Len() int           { return len(events) }
func (events Events) Less(i, j int) bool { return events[i].Id < events[j].Id }
func (events Events) Swap(i, j int)      { events[i], events[j] = events[j], events[i] }

func (events Events) FlatMap() (flats map[string]Events) {
	flats = make(map[string]Events)
	for _, event := range events {
		flat, has := flats[event.AggregateId]
		if has {
			flat = append(flat, event)
		} else {
			flat = make([]*Event, 0, len(events))
			flat = append(flat, event)
			flats[event.AggregateId] = flat
		}
	}
	for _, flat := range flats {
		sort.Sort(flat)
	}
	return
}

func (events Events) Encode() (p []byte, err error) {

	return
}