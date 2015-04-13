package kafka

import (
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"strconv"
	"sync/atomic"
)

type collectionState struct {
	Version string
	Topic   string
	Offset  uint64
}

type KafkaDataLoader struct {
	brokers            []*db.BaseConfig
	writer             db.EventWriter
	checkpoint         db.Checkpointer
	master             sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	state              collectionState
	ckKey              string
	collecting         int32
}

const (
	topicKey       = "Topic"
	partitionKey   = "Partition"
	offsetKey      = "Offset"
	stopped        = 0
	initialStarted = 1
	started        = 2
)

// NewKafaEventloader
// @BaseConfig.AdditionalConfig: contains flushFrequency
// FIXME support more config options
func NewKafkaDataLoader(brokers []*db.BaseConfig, writer db.EventWriter, checkpoint db.Checkpointer) *KafkaDataLoader {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	var brokerList []string
	for i := 0; i < len(brokers); i++ {
		brokerList = append(brokerList, brokers[i].ServerURL)
	}

	master, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		glog.Errorf("Failed to create Kafka consumer, error=%s", err)
		return nil
	}

	partition, _ := strconv.Atoi(brokers[0].AdditionalConfig[partitionKey])
	partition_id := int32(partition)
	topic := brokers[0].AdditionalConfig[topicKey]
	offset := getOffsetForTopic(topic)

	// FIXME consumer groups
	consumer, err := master.ConsumePartition(topic, partition_id, offset)
	if err != nil {
		glog.Errorf("Failed to create Kafka partition consumer for topic=%s, error=%s", topic, err)
		return nil
	}
	consumers := []sarama.PartitionConsumer{consumer}

	return &KafkaDataLoader{
		brokers:            brokers,
		writer:             writer,
		checkpoint:         checkpoint,
		master:             master,
		partitionConsumers: consumers,
		ckKey:              brokers[0].AdditionalConfig[topicKey],
		collecting:         initialStarted,
	}
}

func (loader *KafkaDataLoader) Start() {
	if !atomic.CompareAndSwapInt32(&loader.collecting, initialStarted, started) {
		glog.Info("KafkaEventloader already started or stopped")
		return
	}

	for i, _ := range loader.partitionConsumers {
		go func() {
			for atomic.LoadInt32(&loader.collecting) != stopped {
				for err := range loader.partitionConsumers[i].Errors() {
					glog.Errorf("Encounter error while collecting data from topic=%s, error=%s", loader.brokers[0].AdditionalConfig[topicKey], err)
				}
				break
			}
		}()
	}
}

func (loader *KafkaDataLoader) Stop() {
	if !atomic.CompareAndSwapInt32(&loader.collecting, started, stopped) {
		glog.Info("KafkaDataLoader already stopped")
		return
	}
	loader.master.Close()
	for _, consumer := range loader.partitionConsumers {
		consumer.AsyncClose()
	}
}

func (loader *KafkaDataLoader) CollectData() ([]byte, error) {
	return nil, nil
}

func (loader *KafkaDataLoader) IndexData() error {
	for msg := range loader.partitionConsumers[0].Messages() {
		// FIXME write offset
		metaInfo := map[string]string{}
		event := db.NewEvent(metaInfo, [][]byte{msg.Value})
		loader.writer.WriteEvents(event)
	}
	return nil
}

func getOffsetForTopic(topic string) int64 {
	// strconv.ParseInt(loader.getOffsetForTopic(topic), 10, 8)
	return sarama.OffsetOldest
}
