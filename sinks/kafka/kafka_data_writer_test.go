package kafka

import (
	"github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaDataWriter(t *testing.T) {
	sinkConfig := []base.BaseConfig{
		base.BaseConfig{
			base.ServerURL: "172.16.107.153:9092",
			"Topic":        "DescartesTest",
			"Key":          "MyKey",
			"host":         "my.host.com",
			"user":         "Ken Chen",
		},
	}
	writer := NewKafkaDataWriter(sinkConfig)
	writer.Start()
	writer.Start()
	defer writer.Stop()

	for i := 0; i < 3; i++ {
		metaInfo := map[string]string{
			"Topic": "DescartesTest",
			"Key":   "MyKey",
			"host":  "my.host.com",
			"user":  "Ken Chen",
		}
		rawData := [][]byte{[]byte("sync:a=b,c=d,1=2,3=4"), []byte("sync:1=2,3=4,a=b,c=d")}
		data := base.NewData(metaInfo, rawData)
		asyncrawData := [][]byte{[]byte("aysnc:a=b,c=d,1=2,3=4"), []byte("aysnc:1=2,3=4,a=b,c=d")}
		asyncData := base.NewData(metaInfo, asyncrawData)
		writer.WriteDataSync(data)
		writer.WriteDataAsync(asyncData)
	}
	time.Sleep(time.Second)
}
