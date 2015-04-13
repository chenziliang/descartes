package splunk

import (
	"crypto/tls"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

const (
	hostKey       = "host"
	hostRegexKey  = "host_regex"
	indexKey      = "index"
	sourceKey     = "source"
	sourcetypeKey = "sourcetype"
	syncWriteKey  = "writeSync"
)

type SplunkEventWriter struct {
	splunkdCredentials []*db.BaseConfig
	sessionKeys        map[string]string
	rest               SplunkRest
	eventQ             chan *db.Event
	started            int32
}

func NewSplunkEventWriter(credentials []*db.BaseConfig) *SplunkEventWriter {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}

	return &SplunkEventWriter{
		splunkdCredentials: credentials,
		sessionKeys:        make(map[string]string, len(credentials)),
		rest:               SplunkRest{client},
		eventQ:             make(chan *db.Event, 1000),
	}
}

func (writer *SplunkEventWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.started, 0, 1) {
		glog.Info("SplunkEventWriter already started")
		return
	}

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
	}()
}

func (writer *SplunkEventWriter) Stop() {
	writer.eventQ <- nil
}

func (writer *SplunkEventWriter) WriteEvents(events *db.Event) error {
	if writer.splunkdCredentials[0].AdditionalConfig[syncWriteKey] == "1" {
		return writer.WriteEventsSync(events)
	} else {
		return writer.WriteEventsAsync(events)
	}
}

func (writer *SplunkEventWriter) WriteEventsAsync(events *db.Event) error {
	writer.eventQ <- events
	return nil
}

func (writer *SplunkEventWriter) WriteEventsSync(events *db.Event) error {
	return writer.doWriteEvents(events)
}

func (writer *SplunkEventWriter) doWriteEvents(events *db.Event) error {
	metaProps := url.Values{}
	metaProps.Add(hostKey, events.MetaInfo[hostKey])
	metaProps.Add(indexKey, events.MetaInfo[indexKey])
	metaProps.Add(sourceKey, events.MetaInfo[sourceKey])
	metaProps.Add(sourcetypeKey, events.MetaInfo[sourcetypeKey])

	// FIXME perf issue for bytes concatenation
	data := make([]byte, 0, 4096)
	n := len(events.RawEvents)
	for i := 0; i < n; i++ {
		data = append(data, events.RawEvents[i]...)
		data = append(data, '\n')
	}

	for serverURL, sessionKey := range writer.sessionKeys {
		err := writer.rest.IndexData(serverURL, sessionKey, &metaProps, data)
		if err != nil {
			glog.Errorf("Failed to index data from %s, error=%s", serverURL, err)
			continue
		}
		break
	}

	return nil
}

func (writer *SplunkEventWriter) login() error {
	for _, cred := range writer.splunkdCredentials {
		sessionKey, err := writer.rest.Login(cred.ServerURL, cred.Username, cred.Password)
		if err != nil {
			// FIXME err handling
			continue
		}
		writer.sessionKeys[cred.ServerURL] = sessionKey
	}
	return nil
}
