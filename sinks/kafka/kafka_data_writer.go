package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"strings"
	"sync/atomic"
	"time"
)

type KafkaDataWriter struct {
	brokerConfig  base.BaseConfig
	asyncProducer sarama.AsyncProducer
	syncProducer  sarama.SyncProducer
	state         int32
}

const (
	stopped        = 0
	initialStarted = 1
	started        = 2
)

// NewKafaDataWriter
// @BaseConfig: contains
// base.Topic, base.Key which indicates where to write the data to Kafka
// base.RequireAcks, base.FlushMemory, base.SyncWrite Kafka producer options

func NewKafkaDataWriter(brokerConfig base.BaseConfig) base.DataWriter {
	for _, k := range []string{base.Topic, base.Key, base.Brokers} {
		if val, ok := brokerConfig[k]; !ok || val == "" {
			glog.Errorf("%s config is required", k)
			return nil
		}
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	brokers := strings.Split(brokerConfig[base.Brokers], ";")
	asyncProducer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		glog.Errorf("Failed to create Kafka async producer, error=%s", err)
		return nil
	}

	syncConfig := sarama.NewConfig()
	syncProducer, err := sarama.NewSyncProducer(brokers, syncConfig)
	if err != nil {
		glog.Errorf("Failed to create Kafka sync producer, error=%s", err)
		return nil
	}

	return &KafkaDataWriter{
		brokerConfig:  brokerConfig,
		asyncProducer: asyncProducer,
		syncProducer:  syncProducer,
		state:         initialStarted,
	}
}

func (writer *KafkaDataWriter) Start() {
	if !atomic.CompareAndSwapInt32(&writer.state, initialStarted, started) {
		glog.Infof("KafkaDataWriter already started or stopped")
		return
	}

	go func() {
		for err := range writer.asyncProducer.Errors() {
			glog.Errorf("Kafka AsyncProducer encounter error=%s", err)
		}
	}()
	glog.Infof("KafkaDataWriter started...")
}

func (writer *KafkaDataWriter) Stop() {
	if !atomic.CompareAndSwapInt32(&writer.state, started, stopped) {
		glog.Infof("KafkaDataWriter already stopped")
		return
	}

	writer.syncProducer.Close()
	writer.asyncProducer.AsyncClose()
	glog.Infof("KafkaDataWriter stopped...")
}

func (writer *KafkaDataWriter) WriteData(data *base.Data) error {
	if writer.brokerConfig[base.SyncWrite] == "0" {
		return writer.WriteDataSync(data)
	} else {
		return writer.WriteDataAsync(data)
	}
}

func (writer *KafkaDataWriter) prepareData(data *base.Data) (*sarama.ProducerMessage, error) {
	payload, err := json.Marshal(data)
	if err != nil {
		glog.Errorf("Failed to marshal base.Data object, error=%s", err)
		return nil, err
	}

	msg := &sarama.ProducerMessage{
		Topic: writer.brokerConfig[base.Topic],
		Key:   sarama.StringEncoder(writer.brokerConfig[base.Key]),
		Value: sarama.StringEncoder(payload),
	}
	return msg, err
}

func (writer *KafkaDataWriter) WriteDataAsync(data *base.Data) error {
	msg, err := writer.prepareData(data)
	if err != nil {
		return err
	}

	if atomic.LoadInt32(&writer.state) != stopped {
		writer.asyncProducer.Input() <- msg
	}
	return nil
}

func (writer *KafkaDataWriter) WriteDataSync(data *base.Data) error {
	msg, err := writer.prepareData(data)
	if err != nil {
		return err
	}

	_, _, err = writer.syncProducer.SendMessage(msg)
	// FIXME retry other brokers when failed ?
	if err != nil {
		glog.Errorf("Failed to write data to kafka for topic=%s, partition=%d, key=%s, error=%s",
			msg.Topic, msg.Partition, msg.Key, err)
	}
	return err
}
