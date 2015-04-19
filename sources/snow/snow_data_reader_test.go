package snow

import (
	"bytes"
	"encoding/base64"
	"fmt"
	db "github.com/chenziliang/descartes/base"
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

	additionalConfig := map[string]string{
		"endpoint":             "incident",
		"timestampField":       "sys_updated_on",
		"nextRecordTime":       "2014-03-23+08:19:04",
		"recordCount":          "5",
		db.CheckpointDir:       ".",
		db.CheckpointNamespace: "test",
		db.CheckpointKey:       buf.String() + "_" + "incident",
		db.CheckpointTopic:     ckTopic,
		db.CheckpointPartition: fmt.Sprintf("%d", partition),
	}

	sourceConfig := &db.BaseConfig{
		ServerURL:        "https://ven01034.service-now.com",
		Username:         "admin",
		Password:         "splunk123",
		AdditionalConfig: additionalConfig,
	}

	brokerConfigs := []*db.BaseConfig{
		&db.BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}

	client := db.NewKafkaClient(brokerConfigs, "consumerClient")

	writer := &db.StdoutDataWriter{}
	// ck := db.NewFileCheckpointer()
	ck := db.NewKafkaCheckpointer(client)
	writer.Start()
	dataReader := NewSnowDataReader(sourceConfig, writer, ck)
	dataReader.IndexData()
	time.Sleep(10 * time.Second)
	writer.Stop()
	time.Sleep(time.Second)
}
