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
	syncWriteKey  = "syncWrite"
)

type SplunkDataWriter struct {
	splunkdCredentials []*db.BaseConfig
	sessionKeys        map[string]string
	rest               SplunkRest
	dataQ              chan *db.Data
	started            int32
}

func NewSplunkDataWriter(credentials []*db.BaseConfig) db.DataWriter {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}

	return &SplunkDataWriter{
		splunkdCredentials: credentials,
		sessionKeys:        make(map[string]string, len(credentials)),
		rest:               SplunkRest{client},
		dataQ:              make(chan *db.Data, 1000),
	}
}

func (writer *SplunkDataWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.started, 0, 1) {
		glog.Info("SplunkDataWriter already started")
		return
	}

	go func() {
		writer.login()

		for {
			data := <-writer.dataQ
			if data != nil {
				writer.doWriteData(data)
			} else {
				break
			}
		}
		glog.Info("SplunkDataWriter is going to exit...")
	}()
}

func (writer *SplunkDataWriter) Stop() {
	writer.dataQ <- nil
}

func (writer *SplunkDataWriter) WriteData(data *db.Data) error {
	if writer.splunkdCredentials[0].AdditionalConfig[syncWriteKey] == "1" {
		return writer.WriteDataSync(data)
	} else {
		return writer.WriteDataAsync(data)
	}
}

func (writer *SplunkDataWriter) WriteDataAsync(data *db.Data) error {
	writer.dataQ <- data
	return nil
}

func (writer *SplunkDataWriter) WriteDataSync(data *db.Data) error {
	return writer.doWriteData(data)
}

func (writer *SplunkDataWriter) doWriteData(data *db.Data) error {
	metaProps := url.Values{}
	metaProps.Add(hostKey, data.MetaInfo[hostKey])
	metaProps.Add(indexKey, data.MetaInfo[indexKey])
	metaProps.Add(sourceKey, data.MetaInfo[sourceKey])
	metaProps.Add(sourcetypeKey, data.MetaInfo[sourcetypeKey])

	// FIXME perf issue for bytes concatenation
	allData := make([]byte, 0, 4096)
	n := len(data.RawData)
	for i := 0; i < n; i++ {
		allData = append(allData, data.RawData[i]...)
		allData = append(allData, '\n')
	}

	for serverURL, sessionKey := range writer.sessionKeys {
		err := writer.rest.IndexData(serverURL, sessionKey, &metaProps, allData)
		if err != nil {
			glog.Errorf("Failed to index data from %s, error=%s", serverURL, err)
			continue
		}
		break
	}

	return nil
}

func (writer *SplunkDataWriter) login() error {
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
