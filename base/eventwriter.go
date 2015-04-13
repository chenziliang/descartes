package base

import (
	"fmt"
)

type EventWriter interface {
	Start()
	Stop()
	WriteEvents(events *Event) error // can be sync or async
	WriteEventsSync(events *Event) error
	WriteEventsAsync(events *Event) error
}

type StdoutEventWriter struct {
}

func (d *StdoutEventWriter) Start() {
}

func (d *StdoutEventWriter) Stop() {
}


func (d *StdoutEventWriter) WriteEvents(events *Event) error {
	return d.doWriteEvents(events)
}

func (d *StdoutEventWriter) WriteEventsSync(events *Event) error {
	return d.doWriteEvents(events)
}

func (d *StdoutEventWriter) WriteEventsAsync(events *Event) error {
	return d.doWriteEvents(events)
}

func (d *StdoutEventWriter) doWriteEvents(events *Event) error {
	for i := 0; i < len(events.RawEvents); i++ {
		fmt.Println(events.RawEvents[i])
	}
	return nil
}
