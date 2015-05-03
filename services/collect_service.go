package services

import (
	"encoding/json"
	"fmt"
	"github.com/chenziliang/descartes/base"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
	"github.com/chenziliang/descartes/sinks/memory"
	kafkareader "github.com/chenziliang/descartes/sources/kafka"
	"github.com/golang/glog"
	"os"
	"runtime"
	"sync/atomic"
	"time"
)

type CollectService struct {
	jobFactory     *JobFactory
	brokerConfig   base.BaseConfig
	client         *base.KafkaClient
	jobs           map[string]base.Job         // job key indexed
	host           string
	started        int32
}

const (
	heartbeatInterval = 6 * time.Second
)

func NewCollectService(brokerConfig base.BaseConfig) *CollectService {
	client := base.NewKafkaClient(brokerConfig, "TaskMonitorClient")
	if client == nil {
		return nil
	}

	// FIXME IP ?
	host, err := os.Hostname()
	if err != nil {
		return nil
	}

	return &CollectService{
		jobFactory:     NewJobFactory(),
		client:         client,
		brokerConfig:   brokerConfig,
		jobs:           make(map[string]base.Job, 100),
		host:           host,
		started:        0,
	}
}

func (ss *CollectService) Start() {
	if !atomic.CompareAndSwapInt32(&ss.started, 0, 1) {
		glog.Infof("CollectService already started.")
		return
	}

	go ss.monitorTasks(base.Tasks)
	go ss.doHeartBeats()

	glog.Infof("CollectService started...")
}

func (ss *CollectService) Stop() {
	if !atomic.CompareAndSwapInt32(&ss.started, 1, 0) {
		glog.Infof("CollectService already stopped.")
		return
	}

	ss.jobFactory.CloseClients()
	ss.client.Close()

	for _, job := range ss.jobs {
		job.Stop()
	}
	glog.Infof("CollectService stopped...")
}

func (ss *CollectService) doHeartBeats() {
	brokerConfig := base.BaseConfig{
		base.KafkaBrokers:   ss.brokerConfig[base.KafkaBrokers],
		base.KafkaTopic:     base.TaskStats,
		base.Key:       base.TaskStats,
	}

	writer := kafkawriter.NewKafkaDataWriter(brokerConfig)
	if writer == nil {
		panic("Failed to create kafka writer")
	}
	writer.Start()
	defer writer.Stop()

	stats := map[string]string {
		base.Host: ss.host,
		base.Platform: runtime.GOOS,
		base.App: "",
		base.CpuCount: fmt.Sprintf("%d", runtime.NumCPU()),
		base.Timestamp: "",
	}

	ticker := time.Tick(heartbeatInterval)
	for atomic.LoadInt32(&ss.started) != 0 {
		select {
		case <-ticker:
			stats[base.Timestamp] = fmt.Sprintf("%d", time.Now().UnixNano())
			for _, app := range ss.jobFactory.Apps() {
				stats[base.App] = app
				rawData, _ := json.Marshal(stats)
				// glog.Infof("Send heartbeat host=%s, app=%s", ss.host, app)
				data := &base.Data{
					RawData:  [][]byte{rawData},
				}
				writer.WriteData(data)
			}
		}
	}
}

func (ss *CollectService) monitorTasks(topic string) {
	checkpoint := base.NewNullCheckpointer()
	writer := memory.NewMemoryDataWriter()
	topicPartitions, err := ss.client.TopicPartitions(topic)
	if err != nil {
		panic(fmt.Sprintf("Failed to get partitions for topic=%s", topic))
	}

	for _, partition := range topicPartitions[topic] {
		config := base.BaseConfig{
			base.KafkaTopic:               topic,
			base.KafkaPartition:           fmt.Sprintf("%d", partition),
		    base.UseOffsetNewest:     "1",
		}

		reader := kafkareader.NewKafkaDataReader(ss.client, config, writer, checkpoint)
		if reader == nil {
			panic("Failed to create kafka reader")
		}

		go func(r base.DataReader, w *memory.MemoryDataWriter) {
			r.Start()
			defer r.Stop()
			go r.IndexData()

			for atomic.LoadInt32(&ss.started) != 0 {
				select {
				case data := <-writer.Data():
					ss.handleTasks(data)
				}
			}
		}(reader, writer)
	}
}


// tasks are expected in map[string]string format
func (ss *CollectService) handleTasks(data *base.Data) {
	if _, ok := data.MetaInfo[base.Host]; !ok {
		glog.Errorf("Host is missing in the task=%s", data)
		return
	}

	for _, rawData := range data.RawData {
		taskConfig := make(base.BaseConfig)
		err := json.Unmarshal(rawData, &taskConfig)
		if err != nil {
			glog.Errorf("Unexpected config format, got=%s", string(rawData))
			continue
		}

		if _, ok := taskConfig[base.App]; !ok {
			glog.Errorf("Invalid config, App is missing in the task=%s", taskConfig)
			continue
		}

		if data.MetaInfo[base.Host] != ss.host {
			return
		}

		if _, ok := ss.jobs[taskConfig[base.TaskConfigKey]]; ok {
			glog.Infof("Use cached collector, app=%s", taskConfig[base.App])
		} else {
		    job := ss.jobFactory.CreateJob(taskConfig[base.App], taskConfig)
			if job == nil {
				return
			}
			ss.jobs[taskConfig[base.TaskConfigKey]] = job
			job.Start()
		}
		// glog.Infof("Handle task=%s", taskConfig)
		go ss.jobs[taskConfig[base.TaskConfigKey]].Callback()
	}
}
