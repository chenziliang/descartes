package splunk

import (
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type SplunkDataWriter struct {
	splunkdConfigs []base.BaseConfig
	sessionKeys    [][]string
	rest           SplunkRest
	dataQ          chan *base.Data
	nextSlot       int
	started        int32
}

func NewSplunkDataWriter(configs []base.BaseConfig) base.DataWriter {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}

	writer := &SplunkDataWriter{
		splunkdConfigs: configs,
		sessionKeys:    make([][]string, 0, len(configs)),
		rest:           SplunkRest{client},
		dataQ:          make(chan *base.Data, 1000),
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
	if writer.splunkdConfigs[0][base.SyncWrite] == "0" {
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
	metaProps.Add("host", data.MetaInfo[base.ServerURL])
	metaProps.Add("index", writer.splunkdConfigs[0][base.Index])
	metaProps.Add("source", writer.splunkdConfigs[0][base.Source])
	metaProps.Add("sourcetype", "snow:"+data.MetaInfo["Endpoint"])

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
	for _, cred := range writer.splunkdConfigs {
		sessionKey, err := writer.rest.Login(cred[base.ServerURL], cred[base.Username], cred[base.Password])
		if err != nil {
			failed = append(failed, cred[base.ServerURL])
			continue
		}
		writer.sessionKeys = append(writer.sessionKeys, []string{cred[base.ServerURL], sessionKey})
	}

	if len(writer.sessionKeys) == 0 {
		return errors.New(fmt.Sprintf("Failed to login all of Splunkds=%s", failed))
	}

	return nil
}
