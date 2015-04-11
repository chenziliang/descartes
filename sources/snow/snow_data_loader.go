package snow

import (
	"fmt"
	"strings"
	"bytes"
	"errors"
	"sync/atomic"
	"net/http"
	"io/ioutil"
	"compress/gzip"
	"encoding/base64"
	"github.com/golang/glog"
	db "github.com/chenziliang/descartes/base"
)


type collectionState struct {
	Version string
	NextCollectionTime string
	LastTimeRecords []string
}

type SnowDataLoader struct {
	*db.BaseConfig
	writer db.EventWriter
	checkpoint db.Checkpointer
	collecting int32
	http_client *http.Client
	ckKey string
	state collectionState
}

// NewSnowDataLoader
// @config.AdditionalConfig: shall contain snow "endpoint", "timestampField"
// "nextTimestamp", "recordCount" key/values
func NewSnowDataLoader(
	config *db.BaseConfig, eventWriter db.EventWriter, checkpointer db.Checkpointer) *SnowDataLoader {
	acquiredConfigs := []string {"endpoint", "timestampField", "nextTimestamp"}
	for _, v := range(acquiredConfigs) {
		if v, ok := config.AdditionalConfig[v]; !ok {
			glog.Errorf("%s is missing. It is required by Snow data collection", v)
			return nil
		}
	}

	reader := []byte(config.ServerURL)
	var writer bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &writer)
	defer encoder.Close()
	encoder.Write(reader)

	return &SnowDataLoader {
		BaseConfig: config,
		writer: eventWriter,
		checkpoint: checkpointer,
		collecting: 0,
		http_client: &http.Client{},
		ckKey: writer.String(),
	}
}

func (snow *SnowDataLoader) getURL() string {
	var buffer bytes.Buffer
	buffer.WriteString(snow.ServerURL)
	buffer.WriteString("/")
	buffer.WriteString(snow.AdditionalConfig["endpoint"])
	buffer.WriteString(".do?JSONv2&sysparm_query=")
	buffer.WriteString(snow.AdditionalConfig["timestampField"])
	buffer.WriteString(snow.AdditionalConfig["nextTimestamp"])
	buffer.WriteString("^ORDERBY")
	buffer.WriteString("&sysparm_record_count=" + snow.AdditionalConfig["recordCount"])
	return buffer.String()
}

func (snow *SnowDataLoader) CollectData() ([]byte, error) {
	if !atomic.CompareAndSwapInt32(&snow.collecting, 0, 1) {
		glog.Info("Last data collection for %s has not been done", snow.getURL())
		return nil, nil
	}
	defer atomic.StoreInt32(&snow.collecting, 1)

	req, err := http.NewRequest("GET", snow.getURL(), nil)
	if err != nil {
		glog.Error("Failed to create request, error=", err)
		return nil, err
	}

	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(snow.Username, snow.Password)

	resp, err := snow.http_client.Do(req)
	if err != nil {
		glog.Error("Failed to do request, error=", err)
		return nil, err
	}
	defer resp.Body.Close()

	reader, err := gzip.NewReader(resp.Body)
    if err != nil {
		glog.Error("Failed to create gzip reader, error=", err)
		return nil, err
    }
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Error("Failed to read uncompressed data, error=", err)
		return nil, err
	}
	return body, nil
}

func (snow *SnowDataLoader) IndexData() error {
	data, err := snow.CollectData()
	if err != nil {
		return err
	}

	jobj, err := db.ToJsonObject(data)
	if err != nil {
		return err
	}

	if records, ok := jobj["records"].([]interface{}); ok {
		snow.removeCollectedRecords(records)
		allEvents :=  db.NewEvent(snow.BaseConfig)
		var record []string
		for i := 0; i < len(records); i++ {
			record = record[:0]
			for k, v := range(records[i].(map[string]interface{})) {
				record = append(record, fmt.Sprintf(`%s="%s"`, k, v))
			}
			allEvents.Add(strings.Join(record, ","))
		}

		if len(records) > 0 {
			err := snow.writer.WriteEvents(allEvents)
			if err != nil {
				return err
			}
			return snow.writeCheckpoint(nextTimestamp)
		}
	} else if errDesc, ok := jobj["error"]; ok {
		glog.Errorf("Failed to get data from %s, error=%s", snow.getURL(), errDesc)
		return errors.New(fmt.Sprintf("%+v", errDesc))
	}
	return nil
}

func (snow *SnowDataLoader) removeCollectedRecords(records []interface{}) {
	lastCheckpoint := snow.getCheckpoint()
}

func (snow *SnowDataLoader) writeCheckpoint(nextTimestamp string) error {
	return snow.checkpoint.WriteCheckpoint(snow.ckKey + "_" + snow.AdditionalConfig["endpoint"], []byte("xxx"))
}

func (snow *SnowDataLoader) getCheckpoint() {
}
