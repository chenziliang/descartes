package splunk


import (
	"fmt"
	"github.com/golang/glog"
	db "github.com/chenziliang/descartes/base"
)

type SplunkEventWriter struct {
	splunkdCredentials []*db.BaseConfig
	sessionKeys []string
	eventQ chan *db.Event
	started bool
}

func NewSplunkEventWriter(credentials []*db.BaseConfig) *SplunkEventWriter {
	return &SplunkEventWriter {
		splunkdCredentials: credentials,
		eventQ: make(chan *db.Event, 1000),
	}
}

func (writer *SplunkEventWriter) Start() {
	if writer.started == true {
		return
	}
	writer.started = true

	go func() {
        writer.login()

		for {
			event := <-writer.eventQ
			if event != nil {
				writer.doWriteEvents(event)
			} else {
				break
			}
		}
		glog.Info("SplunkEventWriter is going to exit...")
	} ()
}

func (writer *SplunkEventWriter) Stop() {
	writer.eventQ <- nil
}

func (writer *SplunkEventWriter) WriteEvents(events *db.Event) error {
	writer.eventQ <- events
	return nil
}

func (writer *SplunkEventWriter) doWriteEvents(events *db.Event) error {
	rawEvents := events.RawEvents()
	n := len(rawEvents)
	for i := 0; i < n; i++ {
		fmt.Println(rawEvents[i])
	}
	return nil
}

func (writer *SplunkEventWriter) login() error {
	for _ = range(writer.splunkdCredentials) {
	}
	return nil
}
