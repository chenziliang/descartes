package base

import (
	// "net/http"
	"fmt"
	"github.com/golang/glog"
)


type EventWriter interface {
	Start()
	Stop()
	WriteEvents(events string) error
}


type SplunkdCredentials struct {
	URL string
	Username string
	Password string
}

type SplunkEventWriter struct {
	credentials []SplunkdCredentials
	sessionKeys []string
	eventQ chan string
	magicToken string
	started bool
}

func NewSplunkEventWriter(credentials []SplunkdCredentials) *SplunkEventWriter {
	return &SplunkEventWriter {
		credentials: credentials,
		eventQ: make(chan string, 1000),
		magicToken: "`@#$%^&*()_+=`",
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
			event := <- writer.eventQ
			if event != writer.magicToken {
				writer.doWriteEvents(event)
			} else {
				break
			}
		}
		glog.Info("SplunkEventWriter is going to exit...")
	} ()
}

func (writer *SplunkEventWriter) Stop() {
	writer.eventQ <- writer.magicToken
}

func (writer *SplunkEventWriter) WriteEvents(events string) error {
	writer.eventQ <- events
	return nil
}

func (writer *SplunkEventWriter) doWriteEvents(events string) error {
	fmt.Println(events)
	return nil
}

func (writer *SplunkEventWriter) login() error {
	return nil
}
