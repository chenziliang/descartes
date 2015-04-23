package kafka

import (
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"strconv"
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

type KafkaDataReader struct {
	client            *base.KafkaClient
	writer            base.DataWriter
	checkpoint        base.Checkpointer
	master            sarama.Consumer
	partitionConsumer sarama.PartitionConsumer
	state             collectionState
	config            base.BaseConfig
	collecting        int32
	startIndexing     int32
}

const (
	stopped        = 0
	initialStarted = 1
	started        = 2
	startIndex     = 3
	maxRetry       = 16
)

// NewKafaDataReader
// FIXME support more config options
func NewKafkaDataReader(client *base.KafkaClient, config base.BaseConfig,
	writer base.DataWriter, checkpoint base.Checkpointer) *KafkaDataReader {
	for _, k := range []string{base.Topic, base.Partition} {
		if _, ok := config[k]; !ok {
			glog.Errorf("Requried %s is missing.", k)
			return nil
		}
	}

	topic, partition := config[base.Topic], config[base.Partition]
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

	state := getCheckpoint(checkpoint, config)
	if state == nil {
		return nil
	}

	pid, _ := strconv.Atoi(config[base.Partition])
	consumer, err := master.ConsumePartition(topic, int32(pid), state.Offset)
	if err != nil {
		glog.Errorf("Failed to create Kafka partition consumer for topic=%s, partition=%s, error=%s", topic, partition, err)
		return nil
	}

	return &KafkaDataReader{
		client:            client,
		writer:            writer,
		checkpoint:        checkpoint,
		master:            master,
		partitionConsumer: consumer,
		state:             *state,
		config:            config,
		collecting:        initialStarted,
	}
}

func (reader *KafkaDataReader) Start() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, initialStarted, started) {
		glog.Infof("KafkDataReader already started or stopped")
		return
	}
	reader.writer.Start()
	reader.checkpoint.Start()
	glog.Infof("KafkDataReader started...")
}

func (reader *KafkaDataReader) Stop() {
	if !atomic.CompareAndSwapInt32(&reader.collecting, started, stopped) {
		glog.Infof("KafkaDataReader already stopped")
		return
	}
	reader.master.Close()
	reader.partitionConsumer.AsyncClose()
	reader.writer.Stop()
	reader.checkpoint.Stop()
	glog.Infof("KafkDataReader stopped...")
}

func (reader *KafkaDataReader) ReadData() ([]byte, error) {
	return nil, nil
}

func (reader *KafkaDataReader) IndexData() error {
	if !atomic.CompareAndSwapInt32(&reader.startIndexing, 0, 1) {
		glog.Infof("KafkDataReader indexing already started.")
		return nil
	}

	var (
		n                                 = 16
		lastMsg   *sarama.ConsumerMessage = nil
		batchs                            = make([]*base.Data, 0, n)
		sleepTime                         = 100 * time.Millisecond
		errMsg                            = "Failed to unmarshal msg, expect marshalled in JSON format of base.Data"
	)

	f := func(msg *sarama.ConsumerMessage, msgs []*base.Data) []*base.Data {
		for _, d := range msgs {
			reader.writeData(msg.Topic, msg.Partition, msg.Offset, d)
		}
		reader.saveOffset(msg.Offset + 1)
		msgs = msgs[:0]
		return msgs
	}

	ticker := time.Tick(10 * time.Second)
	for atomic.LoadInt32(&reader.collecting) != stopped {
		select {
		case err, ok := <-reader.partitionConsumer.Errors():
			if !ok {
				glog.Errorf("Encounter error while collecting data, error=%s", err)
			}

		case msg, ok := <-reader.partitionConsumer.Messages():
			if !ok {
				break
			}
			var data base.Data
			err := json.Unmarshal(msg.Value, &data)
			if err != nil {
				glog.Errorf(errMsg)
				continue
			}

			lastMsg = msg
			batchs = append(batchs, &data)
			if len(batchs) >= n {
				batchs = f(msg, batchs)
			}

		case <-ticker:
			if lastMsg != nil && len(batchs) > 0 {
				batchs = f(lastMsg, batchs)
			}

		default:
			time.Sleep(sleepTime)
		}
	}
	return nil
}

func (reader *KafkaDataReader) writeData(topic string, partition int32, offset int64, data *base.Data) {
	errMsg := fmt.Sprintf("Failed to write data for topic=%s, partition=%d, offset=%d",
		topic, partition, offset)
	var i int
	for i = 0; i < maxRetry; i++ {
		err := reader.writer.WriteData(data)
		if err != nil {
			glog.Errorf(errMsg)
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if i >= maxRetry {
		panic(errMsg)
	}
}

func (reader *KafkaDataReader) saveOffset(offset int64) {
	var newState collectionState = reader.state
	newState.Offset = offset

	errMsg := fmt.Sprintf("Failed to marshal/write checkpoint for consumer group=%s, topic=%s, parition=%d, offset=%d",
		newState.ConsumerGroup, newState.Topic, newState.Partition, offset)
	var i int
	for i = 0; i < maxRetry; i++ {
		data, err := json.Marshal(&newState)
		if err != nil {
			glog.Errorf(errMsg)
			continue
		}

		err = reader.checkpoint.WriteCheckpoint(reader.config, data)
		if err != nil {
			glog.Errorf(errMsg)
		} else {
			break
		}
	}

	if i == maxRetry {
		panic(errMsg)
	}
}

func getCheckpoint(checkpoint base.Checkpointer, config base.BaseConfig) *collectionState {
	data, err := checkpoint.GetCheckpoint(config)
	if err != nil {
		glog.Errorf("Failed to get last checkpoint, error=%s", err)
		return nil
	}

	pid, err := strconv.Atoi(config[base.Partition])
	if err != nil {
		glog.Errorf("Failed to convert %s to a partition integer, error=%s", config[base.Partition], err)
		return nil
	}
	state := collectionState{
		Version:       "1",
		ConsumerGroup: config[base.ConsumerGroup],
		Topic:         config[base.Topic],
		Partition:     int32(pid),
		Offset:        sarama.OffsetOldest,
	}

	if data != nil {
		err = json.Unmarshal(data, &state)
		if err != nil {
			glog.Errorf("Failed to unmarshal data=%s, doesn't conform to collectionState", string(data))
			return nil
		}
	}

	glog.Infof("No checkpoint found for consumer group=%s, topic=%s, partition=%s, use oldest one",
		config[base.ConsumerGroup], config[base.Topic], config[base.Partition])
	return &state
}
