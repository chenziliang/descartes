package base


import (
	"fmt"
)

type EventWriter interface {
	Start()
	Stop()
	WriteEventsSync(events *Event) error
	WriteEventsAsync(events *Event) error
}


type StdoutEventWriter struct {
}

func (d *StdoutEventWriter) Start() {
}

func (d *StdoutEventWriter) Stop() {
}

func (d *StdoutEventWriter) WriteEventsSync(events *Event) error {
	return d.doWriteEvents(events)
}

func (d *StdoutEventWriter) WriteEventsASync(events *Event) error {
	return d.doWriteEvents(events)
}

func (d *StdoutEventWriter) doWriteEvents(events *Event) error {
	for i := 0; i < len(events.RawEvents); i++ {
	    fmt.Println(events.RawEvents[i])
	}
	return nil
}
