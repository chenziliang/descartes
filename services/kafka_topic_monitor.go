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
	topicConfigs        map[string]base.BaseConfig
	configChan          chan base.BaseConfig
	started             int32
}

// For now, only support one kafka cluster
func NewKafkaMetaDataMonitor(config base.BaseConfig, ss *ScheduleService) *KafkaMetaDataMonitor {
	client := base.NewKafkaClient(config, "MonitorClient")

	return &KafkaMetaDataMonitor{
		ss:                  ss,
		client:              client,
		topicPartitions:     make(map[string]map[int32]bool, 10),
		topicConfigs:		 make(map[string]base.BaseConfig, 10),
		configChan:		     make(chan base.BaseConfig, 10),
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

func (mon *KafkaMetaDataMonitor) AddTopicConfig(config base.BaseConfig) {
	mon.configChan <- config
}

func (mon *KafkaMetaDataMonitor) doAddTopicConfig(config base.BaseConfig) {
	glog.Infof("Add topic=%s", config[base.Topic])
	if _, ok := config[base.Topic]; !ok {
		glog.Errorf("Topic is missing in config=%s", config)
		return
	}
	mon.topicConfigs[config[base.Topic]] = config
}

func (mon *KafkaMetaDataMonitor) monitorNewTopicPartitions() {
    ticker := time.Tick(30 * time.Second)
	for atomic.LoadInt32(&mon.started) != 0 {
		select {
		case <-ticker:
			mon.client.Client().RefreshMetadata()
			latestTopicPartitions, err := mon.client.TopicPartitions("")
			if err != nil {
				continue
			}
			mon.checkNewTopicPartitions(latestTopicPartitions)

		case config := <-mon.configChan:
			mon.doAddTopicConfig(config)
		}
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

	if _, ok := mon.topicConfigs[topic]; !ok {
		glog.Errorf("Topic=%s is not registed yet, do nothing", topic)
		return false
	}

	topicConfig := mon.topicConfigs[topic]
	config := make(base.BaseConfig, len(topicConfig))
	for k, v := range topicConfig {
		config[k] = v
	}
	config[base.Brokers] = strings.Join(mon.client.BrokerIPs(), ";")
	config[base.App] = base.KafkaApp
	config[base.TaskConfigKey] = topic + "_" + fmt.Sprintf("%d", partition)
	config[base.Partition] = fmt.Sprintf("%d", partition)

	mon.ss.AddJob(base.TaskConfig, config)
	glog.Infof("Handle new topic=%s, partition=%d", topic, partition)
	return true
}
