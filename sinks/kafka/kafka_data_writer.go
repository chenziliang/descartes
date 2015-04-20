package kafka

import (
	"encoding/json"
	"github.com/Shopify/sarama"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"sync/atomic"
	"time"
)

type KafkaDataWriter struct {
	brokers       []*base.BaseConfig
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
// @BaseConfig.AdditionalConfig: contains
// base.Topic, base.Key which indicates where to write the data to Kafka
// base.RequireAcks, base.FlushMemory, base.SyncWrite Kafka producer options

func NewKafkaDataWriter(brokers []*base.BaseConfig) base.DataWriter {
	if len(brokers) == 0 {
		glog.Errorf("Empty configs passed in")
		return nil
	}

	for _, k := range []string{base.Topic, base.Key} {
		if _, ok := brokers[0].AdditionalConfig[k]; !ok {
			glog.Errorf("%s config is required", k)
			return nil
		}
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Flush.Frequency = 500 * time.Millisecond
	var brokerList []string
	for i := 0; i < len(brokers); i++ {
		brokerList = append(brokerList, brokers[i].ServerURL)
	}

	asyncProducer, err := sarama.NewAsyncProducer(brokerList, config)
	if err != nil {
		glog.Errorf("Failed to create Kafka async producer, error=%s", err)
		return nil
	}

	syncConfig := sarama.NewConfig()
	syncProducer, err := sarama.NewSyncProducer(brokerList, syncConfig)
	if err != nil {
		glog.Errorf("Failed to create Kafka sync producer, error=%s", err)
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
	if writer.brokers[0].AdditionalConfig[base.SyncWrite] == "1" {
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
		Topic: writer.brokers[0].AdditionalConfig[base.Topic],
		Key:   sarama.StringEncoder(writer.brokers[0].AdditionalConfig[base.Key]),
		Value: sarama.StringEncoder(payload),
	}
	return msg, err
}

func (writer *KafkaDataWriter) WriteDataAsync(data *base.Data) error {
	msg, err := writer.prepareData(data)
	if err != nil {
		return err
	}
	writer.asyncProducer.Input() <- msg
	return nil
}

func (writer *KafkaDataWriter) WriteDataSync(data *base.Data) error {
	msg, err := writer.prepareData(data)
	if err != nil {
		return err
	}

	_, _, err = writer.syncProducer.SendMessage(msg)
	// FIXME retry other brokers when failed ?
	// glog.Errorf("topic=%s, partition=%d, offset=%d, error=%s", msg.Topic, partition, offset, err)
	if err != nil {
		glog.Errorf("Failed to write data to kafka for topic=%s, partition=%d, key=%s, error=%s",
			msg.Topic, msg.Partition, msg.Key, err)
	}
	return err
}
