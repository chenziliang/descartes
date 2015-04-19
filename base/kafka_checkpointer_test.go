package base

import (
	"fmt"
	"testing"
)

func TestKafkaCheckponter(t *testing.T) {
	brokerConfigs := []*BaseConfig{
		&BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}
	client := NewKafkaClient(brokerConfigs, "consumerClient")

	keyInfo := map[string]string{
		"Topic":     "CheckpointTestTopic",
		"Partition": "0",
		"Key":       "",
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

	topicPartitions, err := client.TopicPartitions("")
	if err != nil {
		t.Errorf("Failed to get topic and partitions, error=%s", err)
	}
	fmt.Println(topicPartitions)
}
