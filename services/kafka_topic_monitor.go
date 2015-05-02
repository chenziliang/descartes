package services

import (
	"fmt"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"strings"
	"sync/atomic"
	"time"
)


type KafkaMetaDataMonitor struct {
	ss                  *ScheduleService
	client              *base.KafkaClient
	topicPartitions     map[string]map[int32]bool
	started             int32
}

// TODO
// For now, only support one kafka cluster
func NewKafkaMetaDataMonitor(config base.BaseConfig, ss *ScheduleService) *KafkaMetaDataMonitor {
	client := base.NewKafkaClient(config, "MonitorClient")

	return &KafkaMetaDataMonitor{
		ss:                  ss,
		client:              client,
		topicPartitions:     make(map[string]map[int32]bool, 10),
	}
}

func (mon *KafkaMetaDataMonitor) Start() {
	if !atomic.CompareAndSwapInt32(&mon.started, 0, 1) {
		glog.Infof("KafkaMetaDataMonitor already started.")
		return
	}

	go mon.monitorNewTopicPartitions()

	glog.Infof("KafkaMetaDataMonitor started...")
}

func (mon *KafkaMetaDataMonitor) Stop() {
	if !atomic.CompareAndSwapInt32(&mon.started, 1, 0) {
		glog.Infof("KafkaMetaDataMonitor already stopped.")
		return
	}
	mon.client.Close()

	glog.Infof("KafkaMetaDataMonitor stopped...")
}

func (mon *KafkaMetaDataMonitor) monitorNewTopicPartitions() {
	checkInterval := 30 * time.Second

	for atomic.LoadInt32(&mon.started) != 0 {
		mon.client.Client().RefreshMetadata()
		latestTopicPartitions, err := mon.client.TopicPartitions("")
		if err != nil {
			continue
		}
		mon.checkNewTopicPartitions(latestTopicPartitions)
		time.Sleep(checkInterval)
	}
}

func (mon *KafkaMetaDataMonitor) checkNewTopicPartitions(latestTopicPartitions map[string][]int32) {
	for topic, partitions := range latestTopicPartitions {
		for _, id := range partitions {
			if _, ok := mon.topicPartitions[topic]; !ok {
				mon.topicPartitions[topic] = make(map[int32]bool, len(partitions))
			}

			if _, ok := mon.topicPartitions[topic][id]; !ok {
				if !strings.HasSuffix(topic, "_ckpt") && !strings.HasPrefix(topic, "_") {
					glog.Infof("Found new topic=%s, partition=%d", topic, id)
				}

				res := mon.handleNewPartition(topic, id)
				if res {
					mon.topicPartitions[topic][id] = true
				}
			}
		}
	}
}

func (mon *KafkaMetaDataMonitor) handleNewPartition(topic string, partition int32) bool {
	if strings.HasSuffix(topic, "_ckpt") || strings.HasPrefix(topic, "_") {
		return false
	}

	// FIXME for the target ServerURL when new partition is found
	config := base.BaseConfig {
		base.ServerURL: "https://localhost:8089",
		base.Username: "admin",
		base.Password: "admin",
		base.Index: "main",
		base.Source: "descartes",
		base.Brokers: strings.Join(mon.client.BrokerIPs(), ";"),
		base.App: base.KafkaApp,
		base.Topic: topic,
		base.TaskConfigKey: topic + "_" + fmt.Sprintf("%d", partition),
		base.Partition: fmt.Sprintf("%d", partition),
		base.Interval: "600",
	}
	mon.ss.AddJob(base.TaskConfig, config)
	glog.Infof("Handle new topic=%s, partition=%d", topic, partition)
	return true
}
