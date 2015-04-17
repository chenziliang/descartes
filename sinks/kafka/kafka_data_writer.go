package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"sync/atomic"
	"time"
)

type KafkaDataWriter struct {
	brokers       []*db.BaseConfig
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
	state         int32
}

const (
	requireAcksKey    = "RequiredAcks"
	flushFrequencyKey = "FlushFreqency"
	topicKey          = "Topic"
	keyKey            = "Key"
	stopped           = 0
	initialStarted    = 1
	started           = 2
)

// NewKafaDataWriter
// @BaseConfig.AdditionalConfig: contains flushFrequency
// FIXME support more config options
func NewKafkaDataWriter(brokers []*db.BaseConfig) *KafkaDataWriter {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	var brokerList []string
	for i := 0; i < len(brokers); i++ {
		brokerList = append(brokerList, brokers[i].ServerURL)
	}

	asyncProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		glog.Error("Failed to create Kafka async producer, error=", err)
		return nil
	}

	syncConfig := sarama.NewConfig()
	syncProducer, err := sarama.NewSyncProducer(brokerList, syncConfig)
	if err != nil {
		glog.Error("Failed to create Kafka sync producer, error=", err)
		return nil
	}

	return &KafkaDataWriter{
		brokers:       brokers,
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
		state:         initialStarted,
	}
}

func (writer *KafkaDataWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.state, initialStarted, started) {
		glog.Info("KafkaDataWriter already started or stopped")
		return
	}

	go func() {
		for err := range writer.asyncProducer.Errors() {
			glog.Error("Kafka AsyncProducer encounter error=", err)
		}
	}()
}

func (writer *KafkaDataWriter) Stop() {
	if !atomic.CompareAndSwapInt32(&writer.state, started, stopped) {
		glog.Info("KafkaDataWriter already stopped")
		return
	}

	writer.syncProducer.Close()
	writer.asyncProducer.AsyncClose()
}

func (writer *KafkaDataWriter) WriteDataAsync(data *db.Data) error {
	siz := len(data.RawData)
	for i := 0; i < siz; i++ {
		msg := &sarama.ProducerMessage{
			Topic: data.MetaInfo[topicKey],
			Key:   sarama.StringEncoder(data.MetaInfo[keyKey]),
			Value: sarama.StringEncoder(data.RawData[i]),
		}
		writer.asyncProducer.Input() <- msg
	}
	return nil
}

func (writer *KafkaDataWriter) WriteDataSync(data *db.Data) error {
	siz := len(data.RawData)
	for i := 0; i < siz; i++ {
		msg := &sarama.ProducerMessage{
			Topic: data.MetaInfo[topicKey],
			Key:   sarama.StringEncoder(data.MetaInfo[keyKey]),
			Value: sarama.StringEncoder(data.RawData[i]),
		}
		partition, offset, err := writer.syncProducer.SendMessage(msg)
		// FIXME retry other brokers when failed ?
		if err != nil {
			glog.Errorf("Failed to write data to kafka for topic=%s, key=%s, error=%s", msg.Topic, msg.Key, err)
		}
		fmt.Printf("partition=%d, offset=%d\n", partition, offset)
	}
	return nil
}
