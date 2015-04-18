package base

import (
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
)

type KafkaClient struct {
	brokerConfigs []*BaseConfig
	client        sarama.Client
}

func NewKafkaClient(brokerConfigs []*BaseConfig, clientName string) *KafkaClient {
	var brokerIps []string
	for _, brokerConfig := range brokerConfigs {
		brokerIps = append(brokerIps, brokerConfig.ServerURL)
	}

	config := sarama.NewConfig()
	config.ClientID = clientName

	client, err := sarama.NewClient(brokerIps, config)
	if err != nil {
		glog.Errorf("Failed to create KafkaClient name=%s, error=%s", clientName, err)
		return nil
	}

	return &KafkaClient{
		brokerConfigs: brokerConfigs,
		client:        client,
	}
}

func (client *KafkaClient) TopicPartitions(topic string) (map[string][]int32, error) {
	topics := make([]string, 1)
	var err error
	if topic != "" {
		topics = append(topics, topic)
	} else {
		topics, err = client.client.Topics()
		if err != nil {
			glog.Errorf("Failed to get topics from Kafka, error=%s", err)
			return nil, err
		}
	}

	topicPartitions := make(map[string][]int32, len(topics))
	for _, topic := range topics {
		partitions, err := client.client.Partitions(topic)
		if err != nil {
			glog.Errorf("Failed to get partitions for topic=%s from Kafka, error=%s", topic, err)
		}

		_, ok := topicPartitions[topic]
		if !ok {
			topicPartitions[topic] = make([]int32, 0, len(partitions))
		}
		for _, partition := range partitions {
			topicPartitions[topic] = append(topicPartitions[topic], partition)
		}
	}

	if len(topicPartitions) == 0 {
		return nil, err
	}
	return topicPartitions, nil
}

func (client *KafkaClient) GetPartitionOffset(consumerGroup string,
	topic string, partition int32) (int64, error) {
	// 1. Use consumerGroup to get the offset coordinator broker
	// 2. Talk to the coordinator to get the current offset for consumerGroup
	coordinator, err := client.client.Coordinator(consumerGroup)
	if err != nil {
		glog.Errorf("Failed to get coordinator for consumer group=%s, error=%s", consumerGroup, err)
		return 0, err
	}

	req := sarama.OffsetFetchRequest{
		ConsumerGroup: consumerGroup,
		Version:       1,
	}

	req.AddPartition(topic, partition)
	resp, err := coordinator.FetchOffset(&req)
	if err != nil {
		glog.Errorf("Failed to get offset for consumer group=%s, topic=%s, partition=%d, error=%s", consumerGroup, topic, partition, err)
		return 0, err
	}

	offset := resp.Blocks[topic][partition].Offset
	if offset == sarama.OffsetNewest {
		// When consumer group doesn't exist, Kafka server
		// returns sarama.OffsetNewest, but we want OffsetOldest
		offset = sarama.OffsetOldest
	}
	return offset, nil
}

func (client *KafkaClient) Client() sarama.Client {
	return client.client
}
