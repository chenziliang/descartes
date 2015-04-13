package kafka

import (
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaDataLoader(t *testing.T) {
	metaInfo := map[string]string{
		"Topic":    "DescartesTest",
		"Parition": "0",
	}

	sinkConfig := []*db.BaseConfig{
		&db.BaseConfig{
			ServerURL:        "172.16.107.153:9092",
			AdditionalConfig: metaInfo,
		},
	}

	writer := &db.StdoutEventWriter{}
	ck := db.NewFileCheckpointer(".", "test")
	writer.Start()
	dataLoader := NewKafkaDataLoader(sinkConfig, writer, ck)
	go dataLoader.IndexData()
	time.Sleep(3 * time.Second)
	dataLoader.Stop()
	time.Sleep(time.Second)
}
