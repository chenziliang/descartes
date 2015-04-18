package base

import (
	"fmt"
	"testing"
)

func TestKafkaClient(t *testing.T) {
	brokerConfigs := []*BaseConfig{
		&BaseConfig{
			ServerURL: "172.16.107.153:9092",
		},
	}
	client := NewKafkaClient(brokerConfigs, "consumerClient")

	topicPartitions, err := client.TopicPartitions("")
	if err != nil {
		t.Errorf("Failed to get topic and partitions, error=%s", err)
	}
	fmt.Println(topicPartitions)
}
