package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"testing"
	"time"
)

func TestKafkaDataReader(t *testing.T) {
	config := sarama.NewConfig()
	config.ClientID = "consumerClient"
	client, err := sarama.NewClient([]string{"172.16.107.153:9092"}, config)
	if err != nil {
		t.Errorf("Failed to create Kafka Client, error=%s", err)
	}

	topics, err := client.Topics()
	if err != nil {
		t.Errorf("Failed to get topics from Kafka, error=%s", err)
	}
	fmt.Println(topics)

	topicsAndPartitions := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			t.Errorf("Failed to get partitions for topic=%s from Kafka, error=%s", topic, err)
		}

		_, ok := topicsAndPartitions[topic]
		if !ok {
			topicsAndPartitions[topic] = make([]int32, 0, len(partitions))
		}
		for _, partition := range partitions {
			topicsAndPartitions[topic] = append(topicsAndPartitions[topic], partition)
		}
	}
	fmt.Println(topicsAndPartitions)

	additionalInfo := map[string]string{
		"ConsumerGroup": "",
	}
	readerConfig := KafkaDataReaderConfig{
		TopicsAndPartitions: topicsAndPartitions,
		AdditionalConfig:    additionalInfo,
	}

	writer := &db.StdoutDataWriter{}
	ck := db.NewFileCheckpointer(".", "test")
	writer.Start()
	dataReader := NewKafkaDataReader(client, readerConfig, writer, ck)
	go dataReader.IndexData()
	time.Sleep(3 * time.Second)
	dataReader.Stop()
	time.Sleep(time.Second)
}
