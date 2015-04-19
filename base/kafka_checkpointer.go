package base

import (
	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"strconv"
	"sync/atomic"
)

const (
	started = 1
	stopped = 0
)

type KafkaCheckpointer struct {
	client       *KafkaClient
	syncProducer sarama.SyncProducer
	state        int32
}

func NewKafkaCheckpointer(client *KafkaClient) Checkpointer {
	syncConfig := sarama.NewConfig()
	syncProducer, err := sarama.NewSyncProducer(client.BrokerIPs(), syncConfig)
	if err != nil {
		glog.Errorf("Failed to create Kafka sync producer for checkpoint, error=%s", err)
		return nil
	}

	return &KafkaCheckpointer{
		client:       client,
		syncProducer: syncProducer,
		state:        started,
	}
}

func (ck *KafkaCheckpointer) Start() {
}

func (ck *KafkaCheckpointer) Stop() {
	if !atomic.CompareAndSwapInt32(&ck.state, started, stopped) {
		glog.Info("KafkaCheckpointer has already stopped")
		return
	}

	ck.syncProducer.Close()
}

func (ck *KafkaCheckpointer) GetCheckpoint(keyInfo map[string]string) ([]byte, error) {
	partition, _ := strconv.Atoi(keyInfo["Partition"])
	data, err := ck.client.GetLastBlock(keyInfo["Topic"], int32(partition))
	if err != nil {
		glog.Errorf("Failed to get checkpoint for topic=%s, partition=%s", keyInfo["Topic"], keyInfo["Partition"])
		return nil, err
	}
	return data, nil
}

func (ck *KafkaCheckpointer) WriteCheckpoint(keyInfo map[string]string, value []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: keyInfo["Topic"],
		Key:   sarama.StringEncoder(keyInfo["Key"]),
		Value: sarama.StringEncoder(value),
	}
	_, _, err := ck.syncProducer.SendMessage(msg)
	// FIXME retry other brokers when failed ?
	if err != nil {
		glog.Errorf("Failed to write checkpoint to kafka for topic=%s, key=%s, error=%s", msg.Topic, msg.Key, err)
	}

	return err
}

func (ck *KafkaCheckpointer) DeleteCheckpoint(keyInfo map[string]string) error {
	return nil
}
