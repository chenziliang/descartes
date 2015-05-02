package base

import (
	"testing"
)

func TestKafkaCheckponter(t *testing.T) {
	brokerConfig := BaseConfig{
		Brokers: "172.16.107.153:9092",
	}
	client := NewKafkaClient(brokerConfig, "consumerClient")

	keyInfo := map[string]string{
		CheckpointTopic:     "CheckpointTestTopic2",
		CheckpointPartition: "0",
		CheckpointKey:       "",
	}

	data := []byte("abc")
	checkpoint := NewKafkaCheckpointer(client)
	err := checkpoint.WriteCheckpoint(keyInfo, data)
	if err != nil {
		t.Errorf("Failed to write checkpoint, error=%s", err)
	}

	ckData, err := checkpoint.GetCheckpoint(keyInfo)
	if err != nil {
		t.Errorf("Failed to get checkpoint, error=%s", err)
	}

	if string(ckData) != string(data) {
		t.Errorf("Failed to get checkpoint, expected=%s, got=%s", data, ckData)
	}
}
