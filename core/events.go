package core

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"sort"
)

func NewEvent(name string, id uint64, aggregate string, aggregateId string, content []byte) (event *Event) {
	event = &Event{
		Id:          id,
		Aggregate:   aggregate,
		AggregateId: aggregateId,
		Name:        name,
		Content:     content,
	}
	return
}

func NewEventFromBytes(p []byte) (event *Event, err error) {
	event = &Event{}
	err = proto.Unmarshal(p, event)
	if err != nil {
		return
	}
	return
}

func (x *Event) Key() (p []byte) {
	p = []byte(fmt.Sprintf("%s:%s:%s:%d", x.Aggregate, x.AggregateId, x.Name, x.Id))
	return
}

func (x *Event) Encode() (p []byte, err error) {
	p, err = proto.Marshal(x)
	return
}

func NewEventsFromBytes(p []byte) (events *Events, err error) {
	events = &Events{}
	err = proto.Unmarshal(p, events)
	return
}

func (x *Events) Len() int           { return len(x.Events) }
func (x *Events) Less(i, j int) bool { return x.Events[i].Id < x.Events[j].Id }
func (x *Events) Swap(i, j int)      { x.Events[i], x.Events[j] = x.Events[j], x.Events[i] }

func (x *Events) FlatMap() (flats map[string]*Events) {
	flats = make(map[string]*Events)
	for _, event := range x.Events {
		flat, has := flats[event.AggregateId]
		if has {
			flat.Events = append(flat.Events, event)
		} else {
			flat.Events = make([]*Event, 0, len(x.Events))
			flat.Events = append(flat.Events, event)
			flats[event.AggregateId] = flat
		}
	}
	for _, flat := range flats {
		sort.Sort(flat)
	}
	return
}

func (x *Events) Encode() (p []byte, err error) {
	p, err = proto.Marshal(x)
	return
}
