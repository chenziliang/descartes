package services

import (
	"fmt"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"os"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
	"sync/atomic"
	"time"
)

type StatsService struct {
	kafkaClient *base.KafkaClient
	config      base.BaseConfig
	started     int32
}

const (
	dumpInterval = 30 * time.Second
)

func NewStatsService(config base.BaseConfig) *StatsService {
	client := base.NewKafkaClient(config, "StatsMonitorClient")
	if client == nil {
		return nil
	}

	return &StatsService{
		kafkaClient: client,
		config:      config,
	}
}

func (ss *StatsService) Start() {
	if !atomic.CompareAndSwapInt32(&ss.started, 0, 1) {
		glog.Infof("StatsService already started.")
	}

	go ss.dumpCurrentTopics()
}

func (ss *StatsService) Stop() {
	if !atomic.CompareAndSwapInt32(&ss.started, 1, 0) {
		glog.Infof("StatsService already stopped.")
		return
	}
	glog.Infof("StatsService stopped...")
}

func (ss *StatsService) dumpCurrentTopics() {
	brokerConfig := base.BaseConfig{
		base.KafkaBrokers:   ss.config[base.KafkaBrokers],
		base.KafkaTopic:     base.TaskStats,
	}

	writer := kafkawriter.NewKafkaDataWriter(brokerConfig)
	if writer == nil {
		panic("Failed to create kafka writer")
	}
	writer.Start()
	defer writer.Stop()

	hostname, _ := os.Hostname()

	metaInfo := base.BaseConfig{
		base.ServerURL: hostname,
		base.App: base.KafkaApp,
		base.Metric: "topic:partition",
	}

	ticker := time.Tick(dumpInterval)
	for atomic.LoadInt32(&ss.started) != 0 {
		select {
		case <-ticker:
			topicPartitions, err := ss.kafkaClient.TopicPartitions("")
			if err != nil {
				continue
			}

			// FIXME in batch
			data := &base.Data{MetaInfo: metaInfo}
			for topic, partitions := range topicPartitions {
				for _, partition := range partitions {
					evt := fmt.Sprintf("topic=%s,partition=%d", topic, partition)
					data.RawData = [][]byte{[]byte(evt)}
			        writer.WriteData(data)
				}
			}
			// writer.WriteData(data)
		}
	}
}
