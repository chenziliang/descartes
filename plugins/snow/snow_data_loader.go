package snow

import (
	"bytes"
	"net/http"
	"io/ioutil"
	"compress/gzip"
	"github.com/golang/glog"
	db "github.com/chenziliang/descartes/base"
)

type SnowDataLoader struct {
	config db.DataLoaderConfig
	writer db.EventWriter
	checkpoint db.Checkpointer
	endpoint string
	timestampField string
	lastTimestamp string
	collecting bool
	http_client *http.Client
}

func NewSnowDataLoader(
	config db.DataLoaderConfig, eventWriter db.EventWriter, checkpointer db.Checkpointer,
	endpoint, timestampField, timestamp string) * SnowDataLoader {
	return &SnowDataLoader {
		config: config,
		writer: eventWriter,
		checkpoint: checkpointer,
		endpoint: endpoint,
		timestampField: timestampField,
		lastTimestamp: timestamp,
		collecting: false,
		http_client: &http.Client{},
	}
}

func (snow *SnowDataLoader) getURL() string {
	var buffer bytes.Buffer
	buffer.WriteString(snow.config.ServerURL)
	buffer.WriteString("/")
	buffer.WriteString(snow.endpoint)
	buffer.WriteString(".do?JSONv2&sysparm_query=")
	buffer.WriteString(snow.timestampField)
	buffer.WriteString(snow.lastTimestamp)
	buffer.WriteString("^ORDERBY")
	buffer.WriteString("&sysparm_record_count=5000")
	return buffer.String()
}

func (snow *SnowDataLoader) CollectData() (string, error) {
	// FIXME atomic
	if snow.collecting {
		glog.Info("Last data collection for %s has not been done")
		return "", nil
	}
	snow.collecting = true
	defer func() {snow.collecting = false} ()

	req, err := http.NewRequest("GET", snow.getURL(), nil)
	if err != nil {
		glog.Error("Failed to create request ", err)
		return "", err
	}

	req.Header.Add("Accept-Encoding", "gzip")
	req.Header.Add("Accept", "application/json")
	req.SetBasicAuth(snow.config.Username, snow.config.Password)

	resp, err := snow.http_client.Do(req)
	if err != nil {
		glog.Error("Failed to do request ", err)
		return "", err
	}
	defer resp.Body.Close()

	reader, err := gzip.NewReader(resp.Body)
    if err != nil {
		glog.Error("Failed to create gzip reader ", err)
		return "", err
    }
	defer reader.Close()

	body, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Error("Failed to read uncompressed data ", err)
		return "", err
	}
	return string(body), nil
}

func (snow *SnowDataLoader) IndexData() error {
	data, err := snow.CollectData()
	if err != nil {
		return err
	}
	return snow.writer.WriteEvents(data)
}
