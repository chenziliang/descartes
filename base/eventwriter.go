package base


import (
	"fmt"
)

type EventWriter interface {
	Start()
	Stop()
	WriteEvents(events *Event) error
}


type StdoutEventWriter struct {
}

func (d *StdoutEventWriter) Start() {
}

func (d *StdoutEventWriter) Stop() {
}

func (d *StdoutEventWriter) WriteEvents(events *Event) error {
	rawEvents := events.RawEvents()
	for i := 0; i < len(rawEvents); i++ {
	    fmt.Println(rawEvents[i])
	}
	return nil
}
