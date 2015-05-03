package services

import (
	"encoding/json"
	"fmt"
	"github.com/chenziliang/descartes/base"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
	"github.com/chenziliang/descartes/sinks/memory"
	kafkareader "github.com/chenziliang/descartes/sources/kafka"
	"github.com/golang/glog"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type ScheduleService struct {
	jobFactory     *JobFactory
	jobScheduler   *base.Scheduler
	client         *base.KafkaClient
	partitionMonitor  *KafkaMetaDataMonitor
	brokerConfig   base.BaseConfig
	jobConfigs     map[string]base.BaseConfig            // job key indexed
	jobs           map[string]base.Job                   // job key indexed
	dispatchedJobs map[string][]string                   // job dispatched to which host
	heartBeats     map[string]map[string]base.BaseConfig // ip, app => heartbeat
	heartBeatMutex sync.Mutex
	taskChan       chan base.BaseConfig
	started        int32
}

const (
	heartbeatThreadhold = 2 * int64(6 * time.Second)
)

func NewScheduleService(brokerConfig base.BaseConfig) *ScheduleService {
	client := base.NewKafkaClient(brokerConfig, "TaskMonitorClient")
	if client == nil {
		return nil
	}

	ss := &ScheduleService{
		jobFactory:     NewJobFactory(),
		jobScheduler:   base.NewScheduler(),
		client:         client,
		brokerConfig:   brokerConfig,
		jobConfigs:     make(map[string]base.BaseConfig, 100),
		jobs:           make(map[string]base.Job, 100),
		dispatchedJobs: make(map[string][]string),
		heartBeats:     make(map[string]map[string]base.BaseConfig, 100),
		taskChan:       make(chan base.BaseConfig, 100),
		started:        0,
	}
	ss.jobFactory.RegisterJobCreationHandler(base.TaskConfig, ss.createTaskDispatchJob)
	ss.partitionMonitor = NewKafkaMetaDataMonitor(brokerConfig, ss)
	return ss
}

func (ss *ScheduleService) Start() {
	if !atomic.CompareAndSwapInt32(&ss.started, 0, 1) {
		glog.Infof("ScheduleService already started.")
		return
	}

	ss.jobScheduler.Start()
	go ss.monitorTasks()
	go ss.monitorHeartBeats()
	go ss.publishTask()

	glog.Infof("ScheduleService started...")
}

func (ss *ScheduleService) Stop() {
	if !atomic.CompareAndSwapInt32(&ss.started, 1, 0) {
		glog.Infof("ScheduleService already stopped.")
		return
	}

	ss.jobScheduler.Stop()
	ss.partitionMonitor.Stop()
	ss.jobFactory.CloseClients()
	ss.client.Close()
	glog.Infof("ScheduleService stopped...")
}

func (ss *ScheduleService) createTaskDispatchJob(config base.BaseConfig) base.Job {
	interval, err := strconv.ParseInt(config[base.Interval], 10, 64)
	if err != nil {
		glog.Errorf("Failed to convert %s to integer, error=%s", config[base.Interval], err)
		return nil
	}

	interval = interval * int64(time.Second)
	job := base.NewJob(ss.dispatchTaskToKafka, time.Now().UnixNano(), interval, config)
	return job
}

func (ss *ScheduleService) dispatchTaskToKafka(params base.JobParam) error {
	config := params.(base.BaseConfig)
	ss.taskChan <- config
	return nil
}

func (ss *ScheduleService) publishTask() {
	// FIXME base.Key
	brokerConfig := base.BaseConfig{
		base.KafkaBrokers: ss.brokerConfig[base.KafkaBrokers],
		base.KafkaTopic:   base.Tasks,
		base.Key:          base.Tasks,
	}

	writer := kafkawriter.NewKafkaDataWriter(brokerConfig)
	if writer == nil {
		panic("Failed to create kafka writer")
	}
	writer.Start()
	defer writer.Stop()

	for atomic.LoadInt32(&ss.started) != 0 {
		select {
		case taskConfig := <-ss.taskChan:
			rawData, err := json.Marshal(taskConfig)
			if err != nil {
				glog.Errorf("Failed to marshal task config, error=%s", err)
				continue
			}

			host := ss.getAvailableGatheringHost(taskConfig)
			if host == "" {
				continue
			}

			meta := base.BaseConfig{
				base.Host: host,
			}

			data := &base.Data{
				MetaInfo: meta,
				RawData:  [][]byte{rawData},
			}
			writer.WriteData(data)
		}
	}
}

func (ss *ScheduleService) getAvailableGatheringHost(config base.BaseConfig) string {
	// TODO locality
	var availableHosts []string
	ss.heartBeatMutex.Lock()
	for host, appHeartBeat := range ss.heartBeats {
		if heartBeat, ok := appHeartBeat[config[base.App]]; ok {
			lasttime, _ := strconv.ParseInt(heartBeat[base.Timestamp], 10, 64)
			if time.Now().UnixNano()-lasttime < heartbeatThreadhold {
				availableHosts = append(availableHosts, host)
			} else {
				glog.Errorf("Host=%s, App=%s has lost the heartbeat", host, config[base.App])
			}
		}
	}
	ss.heartBeatMutex.Unlock()

	if len(availableHosts) > 0 {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		return availableHosts[r.Int()%len(availableHosts)]
	} else {
		glog.Errorf("All Hosts for App=%s have lost heartbeat, ignore this task=%s",
		            config[base.App], config)
	}

	return ""
}

func (ss *ScheduleService) doMonitor(topic string) {
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
			base.CheckpointTopic:     topic + "ckpt",
			base.CheckpointKey:       topic + "ckpt",
			base.CheckpointPartition: "0",
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
					ss.handleMonitorData(data, topic)
				}
			}
		}(reader, writer)
	}
}

func (ss *ScheduleService) handleMonitorData(data *base.Data, topic string) {
	switch topic {
	case base.TaskConfig:
		ss.handleTasks(data)
	case base.TaskStats:
		ss.handleTaskStats(data)
	default:
		glog.Errorf("Unknown topic=%s", topic)
	}
}

func (ss *ScheduleService) monitorHeartBeats() {
	ss.doMonitor(base.TaskStats)
}

// data is a map and is expected to have the following keys
// base.Host, base.App, base.Platform, base.CpuCount, base.Timestamp
func (ss *ScheduleService) handleTaskStats(data *base.Data) {
	required := []string{base.Host, base.App, base.Platform, base.CpuCount, base.Timestamp}
	for _, rawData := range data.RawData {
		heartBeat := make(base.BaseConfig)
		err := json.Unmarshal(rawData, &heartBeat)
		if err != nil {
			glog.Errorf("Unexpected heartbeat format, got=%s", string(rawData))
			continue
		}

		for _, k := range required {
			if _, ok := heartBeat[k]; !ok {
				glog.Errorf("Invalid heartbeat, expect %s in the config, got=%s", k, heartBeat)
				continue
			}
		}
		// glog.Infof("Got heartbeat from host=%s, app=%s", heartBeat[base.Host], heartBeat[base.App])
		heartBeat[base.Timestamp] = fmt.Sprintf("%d", time.Now().UnixNano())

		ss.heartBeatMutex.Lock()
		if ss.heartBeats[heartBeat[base.Host]] == nil {
			ss.heartBeats[heartBeat[base.Host]] = make(map[string]base.BaseConfig)
		}
		ss.heartBeats[heartBeat[base.Host]][heartBeat[base.App]] = heartBeat
		ss.heartBeatMutex.Unlock()
	}
}

func (ss *ScheduleService) monitorTasks() {
	<-time.After(time.Duration(heartbeatThreadhold))
	ss.partitionMonitor.Start()
	ss.doMonitor(base.TaskConfig)
}

// tasks are expected in map[string]string/base.BaseConfig format
func (ss *ScheduleService) handleTasks(data *base.Data) {
	required := []string{base.TaskConfigAction, base.TaskConfigKey}
	for _, rawData := range data.RawData {
		taskConfig := make(base.BaseConfig)
		err := json.Unmarshal(rawData, &taskConfig)
		if err != nil {
			glog.Errorf("Unexpected config format, got=%s", string(rawData))
			continue
		}

		for _, k := range required {
			if _, ok := taskConfig[k]; !ok {
				glog.Errorf("Invalid config, expect %s in the config, got=%s", k, taskConfig)
				continue
			}
		}

		switch taskConfig[base.TaskConfigAction] {
		case base.TaskConfigNew:
			ss.handleNewTask(taskConfig)
		case base.TaskConfigDelete:
			ss.handleDeleteTask(taskConfig)
		case base.TaskConfigUpdate:
			ss.handleUpdateTask(taskConfig)
		default:
			glog.Errorf("Invalid config config=%s", taskConfig)
		}
	}
}

func (ss *ScheduleService) handleNewTask(config base.BaseConfig) {
	key := config[base.TaskConfigKey]
	if _, ok := ss.jobs[key]; ok {
		glog.Errorf("%s already exists", config)
		return
	}

	// This is an exception
	if config[base.App] == base.KafkaApp {
		ss.partitionMonitor.AddTopicConfig(config)
		return
	}

	job := ss.AddJob(base.TaskConfig, config)
	if job == nil {
	    return
	}
	ss.jobs[key] = job
	ss.jobConfigs[key] = config
}

func (ss *ScheduleService) handleDeleteTask(config base.BaseConfig) {
	key := config[base.TaskConfigKey]
	if _, ok := ss.jobs[key]; !ok {
		glog.Errorf("%s doesn't already exists", config)
		return
	}

	job := ss.jobs[key]
	ss.RemoveJob(job)

	delete(ss.jobs, key)
	delete(ss.jobConfigs, key)
}

func (ss *ScheduleService) handleUpdateTask(config base.BaseConfig) {
	ss.handleDeleteTask(config)
	ss.handleNewTask(config)
}

func (ss *ScheduleService) AddJob(app string, config base.BaseConfig) base.Job {
	job := ss.jobFactory.CreateJob(app, config)
	if job != nil {
		ss.jobScheduler.AddJobs([]base.Job{job})
	} else {
		glog.Errorf("Failed to create job for app=%s, config=%+v", app, config)
	}
	return job
}

func (ss *ScheduleService) RemoveJob(job base.Job) {
	ss.jobScheduler.RemoveJobs([]base.Job{job})
}

func (ss *ScheduleService) UpdateJob(job base.Job) {
	ss.jobScheduler.UpdateJobs([]base.Job{job})
}
