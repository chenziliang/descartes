package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	db "github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"sync/atomic"
	"time"
)

type KafkaEventWriter struct {
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

// NewKafaEventWriter
// @BaseConfig.AdditionalConfig: contains flushFrequency
// FIXME support more config options
func NewKafkaEventWriter(brokers []*db.BaseConfig) *KafkaEventWriter {
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

	return &KafkaEventWriter{
		brokers:       brokers,
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
		state:         initialStarted,
	}
}

func (writer *KafkaEventWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.state, initialStarted, started) {
		glog.Info("KafkaEventWriter already started or stopped")
		return
	}

	go func() {
		for err := range writer.asyncProducer.Errors() {
			glog.Error("Kafka AsyncProducer encounter error=", err)
		}
	}()
}

func (writer *KafkaEventWriter) Stop() {
	if !atomic.CompareAndSwapInt32(&writer.state, started, stopped) {
		glog.Info("KafkaEventWriter already stopped")
		return
	}

	writer.state = stopped
	writer.syncProducer.Close()
	writer.asyncProducer.AsyncClose()
}

func (writer *KafkaEventWriter) WriteEventsAsync(events *db.Event) error {
	siz := len(events.RawEvents)
	for i := 0; i < siz; i++ {
		msg := &sarama.ProducerMessage{
			Topic: events.MetaInfo[topicKey],
			Key:   sarama.StringEncoder(events.MetaInfo[keyKey]),
			Value: sarama.StringEncoder(events.RawEvents[i]),
		}
		writer.asyncProducer.Input() <- msg
	}
	return nil
}

func (writer *KafkaEventWriter) WriteEventsSync(events *db.Event) error {
	siz := len(events.RawEvents)
	for i := 0; i < siz; i++ {
		msg := &sarama.ProducerMessage{
			Topic: events.MetaInfo[topicKey],
			Key:   sarama.StringEncoder(events.MetaInfo[keyKey]),
			Value: sarama.StringEncoder(events.RawEvents[i]),
		}
		partition, offset, err := writer.syncProducer.SendMessage(msg)
		// FIXME retry other brokers when failed ?
		if err != nil {
			glog.Errorf("Failed to write events to kafka for topic=%s, key=%s, error=%s", msg.Topic, msg.Key, err)
		}
		fmt.Printf("partition=%d, offset=%d\n", partition, offset)
	}
	return nil
}
