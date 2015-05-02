package snow

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestSnowDataReader(t *testing.T) {
	reader := []byte("https://ven01034.service-now.com")
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer encoder.Close()
	encoder.Write(reader)

	ckTopic := "SnowCheckpointTopic_0"
	var partition int32 = 0

	sourceConfig := base.BaseConfig{
		base.ServerURL:           "https://ven01034.service-now.com",
		base.Username:            "admin",
		base.Password:            "splunk123",
		endpointKey:              "incident",
		timestampFieldKey:        "sys_updated_on",
		nextRecordTimeKey:        "2014-03-23+08:19:04",
		recordCountKey:           "5",
		base.CheckpointDir:       ".",
		base.CheckpointNamespace: "test",
		base.CheckpointKey:       buf.String() + "_" + "incident",
		base.CheckpointTopic:     ckTopic,
		base.CheckpointPartition: fmt.Sprintf("%d", partition),
	}

	brokerConfig := base.BaseConfig{
		base.Brokers: "172.16.107.153:9092",
	}

	client := base.NewKafkaClient(brokerConfig, "consumerClient")

	writer := &base.StdoutDataWriter{}
	// ck := base.NewFileCheckpointer()
	ck := base.NewKafkaCheckpointer(client)
	writer.Start()
	dataReader := NewSnowDataReader(sourceConfig, writer, ck)
	dataReader.IndexData()
	time.Sleep(10 * time.Second)
	writer.Stop()
	time.Sleep(time.Second)
}
