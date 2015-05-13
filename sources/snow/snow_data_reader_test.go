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
		base.Metric:              "incident",
		timestampFieldKey:        "sys_updated_on",
		nextRecordTimeKey:        "2014-03-23+08:19:04",
		recordCountKey:           "5",
		base.CheckpointDir:       ".",
		base.CheckpointNamespace: "test",
		base.CheckpointKey:       buf.String() + "_" + "incident",
		base.CheckpointTopic:     ckTopic,
		base.CheckpointPartition: fmt.Sprintf("%d", partition),
	}

	cassandraConfig := base.BaseConfig{
		base.CassandraSeeds:    "172.16.107.153:9042",
		base.CassandraKeyspace: "descartes",
		base.CheckpointTable:   "task_ckpts",
	}
	_ = cassandraConfig

	writer := &base.StdoutDataWriter{}
	ck := base.NewFileCheckpointer()
	// ck := base.NewCassandraCheckpointer(cassandraConfig)
	writer.Start()
	dataReader := NewSnowDataReader(sourceConfig, writer, ck)
	dataReader.IndexData()
	time.Sleep(10 * time.Second)
	writer.Stop()
	time.Sleep(time.Second)
}
