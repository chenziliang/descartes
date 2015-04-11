package snow

import (
	"time"
	"testing"
	db "github.com/chenziliang/descartes/base"
)


func TestSnowDataLoader(t *testing.T) {
	additionalConfig := map[string] string {
		"endpoint": "incident",
		"timestampField": "sys_updated_on",
		"nextTimestamp": "2014-03-23+08:19:04",
		"recordCount": "5000",
	}

	sourceConfig := &db.BaseConfig {
		ServerURL: "https://ven01034.service-now.com",
		Username: "admin",
		Password: "splunk123",
		AdditionalConfig: additionalConfig,
	}

	writer := &db.StdoutEventWriter{}
	ck := db.NewFileCheckpointer(".", "test")
	writer.Start()
	dataLoader := NewSnowDataLoader(sourceConfig, writer, ck)
	dataLoader.IndexData()
	writer.Stop()
	time.Sleep(3 * time.Second)
}