package splunk

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type SplunkDataWriter struct {
	splunkdConfig base.BaseConfig
	sessionKeys   [][]string
	rest          SplunkRest
	dataQ         chan *base.Data
	nextSlot      int
	started       int32
}

func NewSplunkDataWriter(config base.BaseConfig) base.DataWriter {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}

	writer := &SplunkDataWriter{
		splunkdConfig: config,
		sessionKeys:   make([][]string, 0),
		rest:          SplunkRest{client},
		dataQ:         make(chan *base.Data, 1000),
	}

	err := writer.login()
	if err != nil {
		return nil
	}
	return writer
}

func (writer *SplunkDataWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.started, 0, 1) {
		glog.Infof("SplunkDataWriter already started")
		return
	}

	go func() {
		for {
			data := <-writer.dataQ
			if data != nil {
				writer.doWriteData(data)
			} else {
				break
			}
		}
		glog.Infof("SplunkDataWriter stopped...")
	}()
	glog.Infof("SplunkDataWriter started...")
}

func (writer *SplunkDataWriter) Stop() {
	writer.dataQ <- nil
}

func (writer *SplunkDataWriter) WriteData(data *base.Data) error {
	if writer.splunkdConfig[base.SyncWrite] == "0" {
		return writer.WriteDataSync(data)
	} else {
		return writer.WriteDataAsync(data)
	}
}

func (writer *SplunkDataWriter) WriteDataAsync(data *base.Data) error {
	writer.dataQ <- data
	return nil
}

func (writer *SplunkDataWriter) WriteDataSync(data *base.Data) error {
	return writer.doWriteData(data)
}

func (writer *SplunkDataWriter) doWriteData(data *base.Data) error {
	metaProps := url.Values{}
	source, sourcetype := SourceAndSourcetype(data.MetaInfo)
	metaProps.Add("host", data.MetaInfo[base.ServerURL])
	metaProps.Add("index", writer.splunkdConfig[base.Index])
	metaProps.Add("source", source)
	metaProps.Add("sourcetype", sourcetype)

	// FIXME perf issue for bytes concatenation
	allData := make([]byte, 0, 4096)
	n := len(data.RawData)
	for i := 0; i < n; i++ {
		allData = append(allData, data.RawData[i]...)
		allData = append(allData, '\n')
	}

	for range writer.sessionKeys {
		writer.nextSlot = (writer.nextSlot + 1) % len(writer.sessionKeys)
		urlSession := writer.sessionKeys[writer.nextSlot]
		err := writer.rest.IndexData(urlSession[0], urlSession[1], &metaProps, allData)
		if err != nil {
			glog.Errorf("Failed to index data to %s, error=%s", urlSession[0], err)
			continue
		}
		break
	}

	// FIXME relogin when fail

	return nil
}

func (writer *SplunkDataWriter) login() error {
	var failed []string
	writer.sessionKeys = writer.sessionKeys[:0]
	config := writer.splunkdConfig
	for _, url := range strings.Split(config[base.ServerURL], ";") {
		sessionKey, err := writer.rest.Login(url, config[base.Username], config[base.Password])
		if err != nil {
			failed = append(failed, url)
			continue
		}
		writer.sessionKeys = append(writer.sessionKeys, []string{url, sessionKey})
	}

	if len(writer.sessionKeys) == 0 {
		return errors.New(fmt.Sprintf("Failed to login all of Splunkds=%s", failed))
	}

	return nil
}
