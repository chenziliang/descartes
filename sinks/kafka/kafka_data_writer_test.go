package kafka

import (
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaDataWriter(t *testing.T) {
	sinkConfig := []*db.BaseConfig{
		&db.BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}
	writer := NewKafkaDataWriter(sinkConfig)
	writer.Start()
	writer.Start()
	defer writer.Stop()

	metaInfo := map[string]string{
		"Topic": "DescartesTest",
		"Key":   "MyKey",
	}
	rawData := [][]byte{[]byte("sync:a=b,c=d,1=2,3=4"), []byte("sync:1=2,3=4,a=b,c=d")}
	data := db.NewData(metaInfo, rawData)
	asyncrawData := [][]byte{[]byte("aysnc:a=b,c=d,1=2,3=4"), []byte("aysnc:1=2,3=4,a=b,c=d")}
	asyncData := db.NewData(metaInfo, asyncrawData)
	writer.WriteDataSync(data)
	writer.WriteDataAsync(asyncData)
	time.Sleep(time.Second)
}
