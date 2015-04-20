package splunk

import (
	"crypto/tls"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type SplunkDataWriter struct {
	splunkdConfigs []*base.BaseConfig
	sessionKeys    map[string]string
	rest           SplunkRest
	dataQ          chan *base.Data
	started        int32
}

func NewSplunkDataWriter(configs []*base.BaseConfig) base.DataWriter {
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	client := &http.Client{Transport: tr, Timeout: 120 * time.Second}

	return &SplunkDataWriter{
		splunkdConfigs: configs,
		sessionKeys:    make(map[string]string, len(configs)),
		rest:           SplunkRest{client},
		dataQ:          make(chan *base.Data, 1000),
	}
}

func (writer *SplunkDataWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.started, 0, 1) {
		glog.Infof("SplunkDataWriter already started")
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
		glog.Infof("SplunkDataWriter stopped...")
	}()
	glog.Infof("SplunkDataWriter started...")
}

func (writer *SplunkDataWriter) Stop() {
	writer.dataQ <- nil
}

func (writer *SplunkDataWriter) WriteData(data *base.Data) error {
	if writer.splunkdConfigs[0].AdditionalConfig[base.SyncWrite] == "1" {
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
	metaProps.Add(base.Host, data.MetaInfo[base.ServerURL])
	metaProps.Add(base.Index, writer.splunkdConfigs[0].AdditionalConfig[base.Index])
	metaProps.Add(base.Source, writer.splunkdConfigs[0].AdditionalConfig[base.Source])
	metaProps.Add(base.Sourcetype, "snow:"+data.MetaInfo["Endpoint"])

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
			glog.Errorf("Failed to index data to %s, error=%s", serverURL, err)
			continue
		}
		break
	}

	return nil
}

func (writer *SplunkDataWriter) login() error {
	for _, cred := range writer.splunkdConfigs {
		sessionKey, err := writer.rest.Login(cred.ServerURL, cred.Username, cred.Password)
		if err != nil {
			// FIXME err handling
			continue
		}
		writer.sessionKeys[cred.ServerURL] = sessionKey
	}
	return nil
}
