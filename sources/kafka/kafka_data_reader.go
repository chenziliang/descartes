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
	config            map[string]string
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
func NewKafkaDataReader(client *base.KafkaClient, config map[string]string,
	writer base.DataWriter, checkpoint base.Checkpointer) *KafkaDataReader {
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

	data, err := checkpoint.GetCheckpoint(config)
	// FIXME checkpoint not exist
	if err != nil {
		//return nil
	}

	pid, _ := strconv.Atoi(partition)
	state := collectionState{
		Version:       "1",
		ConsumerGroup: config[base.ConsumerGroup],
		Topic:         topic,
		Partition:     int32(pid),
		Offset:        sarama.OffsetOldest,
	}

	if data != nil {
		err = json.Unmarshal(data, &state)
		if err != nil {
			return nil
		}
	}

	// fmt.Println(fmt.Sprintf("Get offset=%d for consumer group=%s, topic=%s, partition=%d,", state.Offset, readerConfig.ConsumerGroup, topic, partition))

	consumer, err := master.ConsumePartition(topic, int32(pid), state.Offset)
	if err != nil {
		glog.Errorf("Failed to create Kafka partition consumer for topic=%s, partition=%d, error=%s", topic, partition, err)
		return nil
	}

	return &KafkaDataReader{
		client:            client,
		writer:            writer,
		checkpoint:        checkpoint,
		master:            master,
		partitionConsumer: consumer,
		state:             state,
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

func (reader *KafkaDataReader) CollectData() ([]byte, error) {
	return nil, nil
}

func (reader *KafkaDataReader) IndexData() error {
	var (
		n                               = 128
		lastMsg *sarama.ConsumerMessage = nil
		batchs                          = make([]*base.Data, 0, n)
		errMsg                          = "Failed to unmarshal msg, expect marshalled in JSON format of base.Data"
	)

	f := func(msg *sarama.ConsumerMessage, msgs []*base.Data) []*base.Data {
		for _, d := range msgs {
			reader.writeData(msg.Topic, msg.Partition, msg.Offset, d)
		}
		reader.saveOffset(msg.Offset + 1)
		msgs = msgs[:0]
		return msgs
	}

	ticker := time.Tick(30 * time.Second)
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
			if len(batchs) == n {
				batchs = f(msg, batchs)
			}

		case <-ticker:
			if lastMsg != nil && len(batchs) > 0 {
				batchs = f(lastMsg, batchs)
			}

		default:
			time.Sleep(100 * time.Millisecond)
		}
	}
	return nil
}

func (reader *KafkaDataReader) writeData(topic string, partition int32, offset int64, data *base.Data) {
	var i int
	for i = 0; i < maxRetry; i++ {
		err := reader.writer.WriteData(data)
		if err != nil {
			glog.Errorf("Failed to write data for topic=%s, partition=%d, offset=%d, error=%s", topic, partition, offset, err)
			time.Sleep(time.Second)
		} else {
			break
		}
	}

	if i == maxRetry {
		panic(fmt.Sprintf("Failed to write data for topic=%s, partition=%d, offset=%d", topic, partition, offset))
	}
}

func (reader *KafkaDataReader) saveOffset(offset int64) {
	var newState collectionState = reader.state
	newState.Offset = offset

	errMsg := fmt.Sprintf("Failed to marshal/write checkpoint for consumer group=%s, topic=%s, parition=%d, offset=%d", newState.ConsumerGroup, newState.Topic, newState.Partition, offset)
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
