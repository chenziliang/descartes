package kafka

import (
	// "strconv"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"sync/atomic"
	"time"
)

type collectionState struct {
	Version       string
	ConsumerGroup string
	Topic         string
	Partition     int32
	Offset        int64
}

type KafkaDataReaderConfig struct {
	ConsumerGroup string
	Topic         string
	Partition     int32
}

type KafkaDataReader struct {
	client            *db.KafkaClient
	writer            db.DataWriter
	checkpoint        db.Checkpointer
	master            sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	state             collectionState
	config            KafkaDataReaderConfig
	collecting        int32
}

const (
	stopped        = 0
	initialStarted = 1
	started        = 2
	maxRetry       = 16
)

// NewKafaDataReader
// FIXME support more config options
func NewKafkaDataReader(client *db.KafkaClient, readerConfig KafkaDataReaderConfig, writer db.DataWriter,
	checkpoint db.Checkpointer) *KafkaDataReader {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	topic, partition := readerConfig.Topic, readerConfig.Partition
	master, err := sarama.NewConsumerFromClient(client.Client())
	if err != nil {
		glog.Errorf("Failed to create Kafka consumer, error=%s", err)
		return nil
	}
	defer func() {
		if err != nil {
			master.Close()
		}
	}()

	offset, err := client.GetPartitionOffset(readerConfig.ConsumerGroup, topic, partition)
	if err != nil {
		return nil
	}
	fmt.Println(fmt.Sprintf("Get offset=%d for consumer group=%s, topic=%s, partition=%d,", offset, readerConfig.ConsumerGroup, readerConfig.Topic, readerConfig.Partition))

	consumer, err := master.ConsumePartition(topic, partition, offset)
	if err != nil {
		glog.Errorf("Failed to create Kafka partition consumer for topic=%s, partition=%d, error=%s", topic, partition, err)
		return nil
	}

	state := collectionState{
		Version:       "1",
		ConsumerGroup: readerConfig.ConsumerGroup,
		Topic:         topic,
		Partition:     partition,
		Offset:        offset,
	}

	return &KafkaDataReader{
		client:            client,
		writer:            writer,
		checkpoint:        checkpoint,
		master:            master,
		partitionConsumer: consumer,
		state:             state,
		config:            readerConfig,
		collecting:        initialStarted,
	}
}

func (reader *KafkaDataReader) Start() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, initialStarted, started) {
		glog.Info("KafkDataReader already started or stopped")
		return
	}

	go func() {
		for {
			for err := range reader.partitionConsumer.Errors() {
				glog.Errorf("Encounter error while collecting data, error=%s", err)
			}

			if atomic.LoadInt32(&reader.collecting) == stopped {
				break
			}
		}
	}()
}

func (reader *KafkaDataReader) Stop() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, started, stopped) {
		glog.Info("KafkaDataReader already stopped")
		return
	}
	reader.master.Close()

	reader.partitionConsumer.AsyncClose()
}

func (reader *KafkaDataReader) CollectData() ([]byte, error) {
	return nil, nil
}

func (reader *KafkaDataReader) IndexData() error {
	for msg := range reader.partitionConsumer.Messages() {
		metaInfo := map[string]string{}
		data := db.NewData(metaInfo, [][]byte{msg.Value})
		reader.writeData(msg.Topic, msg.Partition, msg.Offset, data)
		reader.saveOffset(msg.Offset)
	}
	return nil
}

func (reader *KafkaDataReader) writeData(topic string, partition int32, offset int64, data *db.Data) {
	var err error
	for i := 0; i < maxRetry; i++ {
		err = reader.writer.WriteData(data)
		if err != nil {
			glog.Errorf("Failed to write data for topic=%s, partition=%d, offset=%d, error=%s", topic, partition, offset, err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if err != nil {
		panic(fmt.Sprintf("Failed to write data for topic=%s, partition=%d, offset=%d", topic, partition, offset))
	}
}

func (reader *KafkaDataReader) saveOffset(offset int64) {
	var newState collectionState = reader.state
	newState.Offset = offset
	if newState.ConsumerGroup == "" {
		// FIXME generate one
		newState.ConsumerGroup = "generatedOne"
	}
	errMsg := fmt.Sprintf("Failed to marshal/write checkpoint for consumer group=%s, topic=%s, parition=%s, offset=%s", newState.ConsumerGroup, newState.Topic, newState.Partition, offset)

	var i int
	for i = 0; i < maxRetry; i++ {
		data, err := json.Marshal(&newState)
		if err != nil {
			glog.Errorf(errMsg)
			continue
		}
		key := fmt.Sprintf("%s_%s_%d", newState.ConsumerGroup, newState.Topic, newState.Partition)
		err = reader.checkpoint.WriteCheckpoint(map[string]string{"key": key}, data)
		if err != nil {
			glog.Errorf(errMsg)
		} else {
			break
		}
	}

	if i == maxRetry {
		panic(fmt.Sprintf(errMsg))
	}

	broker, err := reader.client.Client().Leader(newState.Topic, newState.Partition)
	if err != nil {
		glog.Errorf("Failed to get leader for topic=%s, partition=%s, reason=%s", newState.Topic, newState.Partition, err)
	}

	req := &sarama.OffsetCommitRequest{
		ConsumerGroup: newState.ConsumerGroup,
		Version:       1,
	}
	req.AddBlock(newState.Topic, newState.Partition, offset, time.Now().UnixNano(), "")
	resp, err := broker.CommitOffset(req)
	fmt.Println(fmt.Sprintf("Commit offset=%d for consumer group=%s, topic=%s, partition=%d,", offset, newState.ConsumerGroup, newState.Topic, newState.Partition))
	if err != nil {
		glog.Errorf("Failed to commit offset for topic=%s, partition=%s, reason=%s,%s", newState.Topic, newState.Partition, err, resp)
	}
}
