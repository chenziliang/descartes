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
	config         base.BaseConfig
	kafkaClient    *base.KafkaClient
	zkClient       *base.ZooKeeperClient
	jobs           map[string]base.Job         // job key indexed
	host           string
	started        int32
}

const (
	heartbeatInterval = 30 * time.Second
)

func NewCollectService(config base.BaseConfig) *CollectService {
	client := base.NewKafkaClient(config, "TaskMonitorClient")
	if client == nil {
		return nil
	}

	zkClient := base.NewZooKeeperClient(config)
	if zkClient == nil {
		return nil
	}

	// FIXME IP ?
	host, err := os.Hostname()
	if err != nil {
		return nil
	}

	return &CollectService{
		jobFactory:     NewJobFactory(),
		kafkaClient:    client,
		zkClient:       zkClient,
		config:			config,
		jobs:           make(map[string]base.Job, 100),
		host:           host,
		started:        0,
	}
}

func (cs *CollectService) Start() {
	if !atomic.CompareAndSwapInt32(&cs.started, 0, 1) {
		glog.Infof("CollectService already started.")
		return
	}

	go cs.monitorTasks(base.Tasks)
	go cs.doHeartbeatsThroughKafka()
	go cs.doHeartbeatsThroughZooKeeper()

	glog.Infof("CollectService started...")
}

func (cs *CollectService) Stop() {
	if !atomic.CompareAndSwapInt32(&cs.started, 1, 0) {
		glog.Infof("CollectService already stopped.")
		return
	}

	cs.jobFactory.CloseClients()
	cs.kafkaClient.Close()
	cs.zkClient.Close()

	for _, job := range cs.jobs {
		job.Stop()
	}
	glog.Infof("CollectService stopped...")
}

func (cs *CollectService) sendHeartbeats() {
	if cs.config[base.Heartbeat] != "kafka" {
		cs.doHeartbeatsThroughZooKeeper()
	} else {
		cs.doHeartbeatsThroughKafka()
	}
}

func (cs *CollectService) doHeartbeats(f func(app string, d *base.Data)) {
	stats := map[string]string {
		base.Host: cs.host,
		base.Platform: runtime.GOOS,
		base.App: "",
		base.CpuCount: fmt.Sprintf("%d", runtime.NumCPU()),
		base.Timestamp: "",
	}

	ticker := time.Tick(heartbeatInterval)
	for atomic.LoadInt32(&cs.started) != 0 {
		select {
		case <-ticker:
			stats[base.Timestamp] = fmt.Sprintf("%d", time.Now().UnixNano())
			for _, app := range cs.jobFactory.Apps() {
				stats[base.App] = app
				rawData, _ := json.Marshal(stats)
				// glog.Infof("Send heartbeat host=%s, app=%s", cs.host, app)
				data := &base.Data{
					RawData:  [][]byte{rawData},
				}
				f(app, data)
			}
		}
	}
}

func (cs *CollectService) doHeartbeatsThroughZooKeeper() {
	// FIXME session expiration/network outage ?
	for _, app := range cs.jobFactory.Apps() {
		node := base.HeartbeatRoot + "/" + cs.host + "!" + app
		cs.zkClient.CreateNode(node, nil, true, true)
	}

	f := func(app string, data *base.Data) {
		node := base.HeartbeatRoot + "/" + cs.host + "!" + app
		cs.zkClient.SetNode(node, data.RawData[0])
	}
	cs.doHeartbeats(f)
}

func (cs *CollectService) doHeartbeatsThroughKafka() {
	brokerConfig := base.BaseConfig{
		base.KafkaBrokers:   cs.config[base.KafkaBrokers],
		base.KafkaTopic:     base.TaskStats,
	}

	writer := kafkawriter.NewKafkaDataWriter(brokerConfig)
	if writer == nil {
		panic("Failed to create kafka writer")
	}
	writer.Start()
	defer writer.Stop()

	f := func(app string, data *base.Data) {
		writer.WriteData(data)
	}
	cs.doHeartbeats(f)
}

func (cs *CollectService) monitorTasks(topic string) {
	checkpoint := base.NewNullCheckpointer()
	writer := memory.NewMemoryDataWriter()
	topicPartitions, err := cs.kafkaClient.TopicPartitions(topic)
	if err != nil {
		panic(fmt.Sprintf("Failed to get partitions for topic=%s", topic))
	}

	for _, partition := range topicPartitions[topic] {
		config := base.BaseConfig{
			base.KafkaTopic:        topic,
			base.KafkaPartition:    fmt.Sprintf("%d", partition),
		    base.UseOffsetNewest:   "1",
		}

		reader := kafkareader.NewKafkaDataReader(cs.kafkaClient, config, writer, checkpoint)
		if reader == nil {
			panic("Failed to create kafka reader")
		}

		go func(r base.DataReader, w *memory.MemoryDataWriter) {
			r.Start()
			defer r.Stop()
			go r.IndexData()

			for atomic.LoadInt32(&cs.started) != 0 {
				select {
				case data := <-writer.Data():
					cs.handleTasks(data)
				}
			}
		}(reader, writer)
	}
}

// tasks are expected in map[string]string format
func (cs *CollectService) handleTasks(data *base.Data) {
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

		if data.MetaInfo[base.Host] != cs.host {
			return
		}

		// FIXME
		var job base.Job
		if taskConfig[base.App] == base.KafkaApp {
			taskConfig[base.LongRun] = "1"
		} else if j, ok := cs.jobs[taskConfig[base.TaskConfigKey]]; ok {
			job = j
			glog.Infof("Use cached collector, app=%s", taskConfig[base.App])
		}

		if job == nil {
			job = cs.jobFactory.CreateJob(taskConfig[base.App], taskConfig)
			if job == nil {
				return
			}
		    glog.Errorf("%s", taskConfig)
			cs.jobs[taskConfig[base.TaskConfigKey]] = job
			job.Start()
		}

		go cs.jobs[taskConfig[base.TaskConfigKey]].Callback()
	}
}
