package kafka

import (
	// "strconv"
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"sync/atomic"
)

type collectionState struct {
	Version string
	Topic   string
	Offset  uint64
}

type KafkaDataReaderConfig struct {
	TopicsAndPartitions map[string][]int32
	AdditionalConfig    map[string]string
}

type KafkaDataReader struct {
	client             sarama.Client
	writer             db.DataWriter
	checkpoint         db.Checkpointer
	master             sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	state              collectionState
	config             KafkaDataReaderConfig
	collecting         int32
}

const (
	consumerGroupKey = "ConsumerGroup"
	stopped          = 0
	initialStarted   = 1
	started          = 2
)

// NewKafaDataReader
// FIXME support more config options
func NewKafkaDataReader(client sarama.Client, readerConfig KafkaDataReaderConfig, writer db.DataWriter,
	checkpoint db.Checkpointer) *KafkaDataReader {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	master, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		glog.Errorf("Failed to create Kafka consumer, error=%s", err)
		return nil
	}

	// FIXME consumer groups
	offsets, err := getOffsetForPartitions(client, readerConfig.AdditionalConfig[consumerGroupKey],
		readerConfig.TopicsAndPartitions)
	if err != nil {
		return nil
	}

	consumers := make([]sarama.PartitionConsumer, 0, len(readerConfig.TopicsAndPartitions))
	for topic, partitions := range readerConfig.TopicsAndPartitions {
		for _, partition := range partitions {
			consumer, err := master.ConsumePartition(topic, partition, offsets[topic][partition])
			if err != nil {
				glog.Errorf("Failed to create Kafka partition consumer for topic=%s, partition=%d, error=%s",
					topic, partition, err)
				// FIXME
				continue
			}
			consumers = append(consumers, consumer)
		}
	}

	if len(consumers) == 0 {
		return nil
	}

	return &KafkaDataReader{
		client:             client,
		writer:             writer,
		checkpoint:         checkpoint,
		master:             master,
		partitionConsumers: consumers,
		config:             readerConfig,
		collecting:         initialStarted,
	}
}

func (reader *KafkaDataReader) Start() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, initialStarted, started) {
		glog.Info("KafkDataReader already started or stopped")
		return
	}

	for i, _ := range reader.partitionConsumers {
		go func() {
			for {
				for err := range reader.partitionConsumers[i].Errors() {
					glog.Errorf("Encounter error while collecting data, error=%s", err)
				}

				if atomic.LoadInt32(&reader.collecting) == stopped {
					break
				}
			}
		}()
	}
}

func (reader *KafkaDataReader) Stop() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, started, stopped) {
		glog.Info("KafkaDataReader already stopped")
		return
	}
	reader.master.Close()
	for _, consumer := range reader.partitionConsumers {
		consumer.AsyncClose()
	}
}

func (reader *KafkaDataReader) CollectData() ([]byte, error) {
	return nil, nil
}

func (reader *KafkaDataReader) IndexData() error {
	for msg := range reader.partitionConsumers[0].Messages() {
		// FIXME write offset
		metaInfo := map[string]string{}
		data := db.NewData(metaInfo, [][]byte{msg.Value})
		reader.writer.WriteData(data)
	}
	return nil
}

func getOffsetForPartitions(client sarama.Client, consumerGroup string,
	topicsAndPartitions map[string][]int32) (map[string]map[int32]int64, error) {
	offsets := make(map[string]map[int32]int64, len(topicsAndPartitions))
	if consumerGroup == "" {
		// Use oldest
		for topic, partitions := range topicsAndPartitions {
			_, ok := offsets[topic]
			if !ok {
				offsets[topic] = make(map[int32]int64)
			}

			for _, partition := range partitions {
				offsets[topic][partition] = sarama.OffsetOldest
			}
		}
		return offsets, nil
	}

	// 1. Use consumerGroup to get the offset coordinator broker
	// 2. Talk to the coordinator to get the current offset for consumerGroup
	coordinator, err := client.Coordinator(consumerGroup)
	if err != nil {
		glog.Errorf("Failed to get coordinator for consumer group=%s, error=%s", consumerGroup, err)
		return nil, err
	}

	req := sarama.OffsetFetchRequest{
		ConsumerGroup: consumerGroup,
		Version:       1,
	}

	for topic, partitions := range topicsAndPartitions {
		for _, partition := range partitions {
			req.AddPartition(topic, partition)
		}
	}

	resp, err := coordinator.FetchOffset(&req)
	if err != nil {
		return nil, err
	}

	for topic, data := range resp.Blocks {
		_, ok := offsets[topic]
		if !ok {
			offsets[topic] = make(map[int32]int64)
		}

		for partition, block := range data {
			if block.Err != sarama.ErrNoError {
				glog.Errorf("Failed to get offset for consumer group=%s, topic=%s, partition=%s, error=%d",
					consumerGroup, topic, partition, block.Err)
				continue
			}
			offsets[topic][partition] = block.Offset
		}
	}
	return offsets, nil
}
