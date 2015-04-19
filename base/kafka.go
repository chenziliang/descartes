package base

import (
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"time"
)

type KafkaClient struct {
	brokerConfigs   []*BaseConfig
	client          sarama.Client
	topicPartitions map[string][]int32
}

const (
	maxRetry = 10
)

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

func (client *KafkaClient) BrokerIPs() []string {
	var brokerIps []string
	for _, brokerConfig := range client.brokerConfigs {
		brokerIps = append(brokerIps, brokerConfig.ServerURL)
	}
	return brokerIps
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
	// race condition
	client.topicPartitions = topicPartitions
	return topicPartitions, nil
}

func (client *KafkaClient) GetConsumerOffset(consumerGroup string,
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

func (client *KafkaClient) GetProducerOffset(topic string, partition int32) (int64, error) {
	leader, err := client.Leader(topic, partition)
	if err != nil {
		return 0, err
	}

	ofreq := &sarama.OffsetRequest{}
	ofreq.AddBlock(topic, partition, time.Now().UnixNano(), 10)

	oresp, err := leader.GetAvailableOffsets(ofreq)
	if err != nil {
		glog.Errorf("Failed to get the available offset for topic=%s, partition=%d, error=%s", topic, partition, err)
		return 0, err
	}

	// offsets are returned in desc order already
	return oresp.GetBlock(topic, partition).Offsets[0], nil
}

func (client *KafkaClient) GetLastBlock(topic string, partition int32) ([]byte, error) {
	lastOffset, err := client.GetProducerOffset(topic, partition)
	if err != nil {
		return nil, err
	}

	leader, err := client.Leader(topic, partition)
	if err != nil {
		return nil, err
	}

	freq := &sarama.FetchRequest{
		MaxWaitTime: 1000, // millisec
		MinBytes:    1,
	}

	if lastOffset > 0 {
		lastOffset -= 1
	}

	freq.AddBlock(topic, partition, lastOffset, 1024)
	fresp, err := leader.Fetch(freq)
	if err != nil {
		glog.Errorf("Failed to get data for topic=%s, partition=%d, error=%s", topic, partition, err)
		return nil, err
	}

	msgBlocks := fresp.Blocks[topic][partition].MsgSet.Messages
	for i := 0; i < len(msgBlocks); i++ {
		block := msgBlocks[i]
		if block.Offset == lastOffset {
			return block.Msg.Value, nil
		}
	}
	return nil, nil
}

func (client *KafkaClient) Leader(topic string, partition int32) (*sarama.Broker, error) {
	var leader *sarama.Broker
	var err error

	for i := 0; i < maxRetry; i++ {
		leader, err = client.client.Leader(topic, partition)
		if err != nil {
			glog.Errorf("Failed to get leader for topic=%s, partition=%d, error=%s", topic, partition, err)
			if client.topicPartitions != nil {
				// Fast break out if topic doesn't exist
				_, ok := client.topicPartitions[topic]
				if !ok {
					return nil, err
				}
			}
			time.Sleep(time.Second)
		} else {
			return leader, err
		}
	}
	return leader, err
}

func (client *KafkaClient) Client() sarama.Client {
	return client.client
}
