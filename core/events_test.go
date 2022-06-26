package core_test

import (
	"fmt"
	"github.com/eventsourcings/aggregated/core"
	"testing"
)

func TestEvent_Encode(t *testing.T) {
	event := core.NewEvent(
		fmt.Sprintf("%d", 10),
		uint64(1),
		fmt.Sprintf("%d", 11),
		fmt.Sprintf("%d", 111),
		[]byte(fmt.Sprintf("%d", 222)),
	)
	p, encodeErr := event.Encode()
	fmt.Println(encodeErr, string(p))
	e, decodeErr := core.NewEventFromBytes(p)
	fmt.Println(fmt.Sprintf("%+v", e), decodeErr)
}

func BenchmarkEvent_Encode(b *testing.B) {
	event := core.NewEvent(
		fmt.Sprintf("%d", 10),
		uint64(1),
		fmt.Sprintf("%d", 11),
		fmt.Sprintf("%d", 111),
		[]byte(fmt.Sprintf("%d", 222)),
	)
	for i := 0; i < b.N; i++ {
		_, _ = event.Encode()
	}
}

func BenchmarkNewEventFromBytes(b *testing.B) {
	event := core.NewEvent(
		fmt.Sprintf("%d", 10),
		uint64(1),
		fmt.Sprintf("%d", 11),
		fmt.Sprintf("%d", 111),
		[]byte(fmt.Sprintf("%d", 222)),
	)
	p, _ := event.Encode()
	for i := 0; i < b.N; i++ {
		_, _ = core.NewEventFromBytes(p)
	}
}

func TestEvents_Encode(t *testing.T) {
	events := &core.Events{}
	for i := 0; i < 5; i++ {
		events.Events = append(events.Events, core.NewEvent(
			fmt.Sprintf("%d", 10),
			uint64(1),
			fmt.Sprintf("%d", 11),
			fmt.Sprintf("%d", 111),
			[]byte(fmt.Sprintf("%d", 222)),
		))
	}
	p, err := events.Encode()
	fmt.Println(string(p), err, len(p))
	events, err = core.NewEventsFromBytes(p)
	fmt.Println(err, fmt.Sprintf("%+v", events))
}

func BenchmarkEvents_Encode(b *testing.B) {
	events := &core.Events{}
	for i := 0; i < 5; i++ {
		events.Events = append(events.Events, core.NewEvent(
			fmt.Sprintf("%d", 10),
			uint64(1),
			fmt.Sprintf("%d", 11),
			fmt.Sprintf("%d", 111),
			[]byte(fmt.Sprintf("%d", 222)),
		))
	}
	for i := 0; i < b.N; i++ {
		_, _ = events.Encode()
	}
}
