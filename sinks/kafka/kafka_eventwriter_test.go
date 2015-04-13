package kafka

import (
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaEventWriter(t *testing.T) {
	sinkConfig := []*db.BaseConfig{
		&db.BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}
	eventWriter := NewKafkaEventWriter(sinkConfig)
	eventWriter.Start()
	eventWriter.Start()
	defer eventWriter.Stop()

	metaInfo := map[string]string{
		"Topic": "DescartesTest",
		"Key":   "MyKey",
	}
	rawEvents := [][]byte{[]byte("sync:a=b,c=d,1=2,3=4"), []byte("sync:1=2,3=4,a=b,c=d")}
	event := db.NewEvent(metaInfo, rawEvents)
	asyncRawEvents := [][]byte{[]byte("aysnc:a=b,c=d,1=2,3=4"), []byte("aysnc:1=2,3=4,a=b,c=d")}
	asyncEvent := db.NewEvent(metaInfo, asyncRawEvents)
	eventWriter.WriteEventsSync(event)
	eventWriter.WriteEventsAsync(asyncEvent)
	time.Sleep(time.Second)
}
