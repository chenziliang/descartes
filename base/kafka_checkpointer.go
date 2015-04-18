package base

import (
	_ "fmt"
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"strconv"
)

type KafkaCheckpointer struct {
	client *KafkaClient
}

func NewKafkaCheckpointer(client *KafkaClient) Checkpointer {
	return &KafkaCheckpointer{
		client: client,
	}
}

func (ck *KafkaCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	master, err := sarama.NewConsumerFromClient(ck.client.Client())
	if err != nil {
		glog.Errorf("Failed to create Kafka consumer, error=%s", err)
		return nil, nil
	}
	defer func() {
		if err != nil {
			master.Close()
		}
	}()

	topic, partition := keyInfo["Topic"], keyInfo["Partition"]
	pid, err := strconv.Atoi(partition)
	_ = pid
	_ = topic
	if err != nil {
		glog.Errorf("Failed to convert partition=%s to int, reason=%s", partition, err)
		return nil, nil
	}

	/*consumer, err := master.ConsumePartition(topic, int32(pid), sarama.OffsetOldest)
	if err != nil {
		glog.Errorf("Failed to create Kafka partition consumer for topic=%s, partition=%s, error=%s", topic, partition, err)
		return nil, nil
	}
	defer consumer.Close()

	fmt.Println(fmt.Sprintf("Get offset=%d for consumer group=%s, topic=%s, partition=%d,", offset, readerConfig.ConsumerGroup, readerConfig.Topic, readerConfig.Partition))*/

	return nil, nil
}

func (ck *KafkaCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	return nil
}

func (ck *KafkaCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	return nil
}
