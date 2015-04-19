package snow

import (
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestSnowDataReader(t *testing.T) {
	additionalConfig := map[string]string{
		"endpoint":       "incident",
		"timestampField": "sys_updated_on",
		"nextRecordTime": "2014-03-23+08:19:04",
		"recordCount":    "5",
	}

	sourceConfig := &db.BaseConfig{
		ServerURL:        "https://ven01034.service-now.com",
		Username:         "admin",
		Password:         "splunk123",
		AdditionalConfig: additionalConfig,
	}

	writer := &db.StdoutDataWriter{}
	ck := db.NewFileCheckpointer(".", "test")
	writer.Start()
	dataReader := NewSnowDataReader(sourceConfig, writer, ck)
	dataReader.IndexData()
	time.Sleep(10 * time.Second)
	writer.Stop()
	time.Sleep(time.Second)
}
