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
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type ScheduleService struct {
	jobFactory     *JobFactory
	jobScheduler   *base.Scheduler
	kafkaClient       *base.KafkaClient
	partitionMonitor  *KafkaMetaDataMonitor
	config         base.BaseConfig
	jobConfigs     map[string]base.BaseConfig            // job key indexed
	jobs           map[string]base.Job                   // job key indexed
	liveCollectors map[string]map[string]base.BaseConfig // ip, app => heartbeat
	liveCollectorsMutex sync.Mutex
	taskChan       chan base.BaseConfig
	zkClient       *base.ZooKeeperClient
	nodeGUID       string
	isLeader       bool
	started        int32
}

const (
	heartbeatThreadhold = 2 * int64(6 * time.Second)
)

// TODO, refactor out the ZooKeeper dependency ?
// config contains: KafkaBrokers, ZooKeeperServers IPs
func NewScheduleService(config base.BaseConfig) *ScheduleService {
	client := base.NewKafkaClient(config, "TaskMonitorClient")
	if client == nil {
		return nil
	}

	zkClient := base.NewZooKeeperClient(config)
	if zkClient == nil {
		return nil
	}

	host, _ := os.Hostname()
	guid, err := zkClient.JoinElection(host)
	if err != nil {
		return nil
	}

	isLeader, err := zkClient.IsLeader(guid)
	if err != nil {
		return nil
	}

	if isLeader {
		glog.Warningf("Take the leader role of scheduler service")
	}

	ss := &ScheduleService{
		jobFactory:     NewJobFactory(),
		jobScheduler:   base.NewScheduler(),
		kafkaClient:    client,
		config:         config,
		jobConfigs:     make(map[string]base.BaseConfig, 100),
		jobs:           make(map[string]base.Job, 100),
		liveCollectors: make(map[string]map[string]base.BaseConfig, 100),
		taskChan:       make(chan base.BaseConfig, 100),
		zkClient:       zkClient,
		nodeGUID:       guid,
		isLeader:       isLeader,
		started:        0,
	}
	ss.jobFactory.RegisterJobCreationHandler(base.TaskConfig, ss.createTaskPublishJob)
	ss.partitionMonitor = NewKafkaMetaDataMonitor(config, ss)
	return ss
}

func (ss *ScheduleService) Start() {
	if !atomic.CompareAndSwapInt32(&ss.started, 0, 1) {
		glog.Infof("ScheduleService already started.")
		return
	}

	ss.jobScheduler.Start()
	go ss.monitorLeaderChanges()
	go ss.monitorTasks()
	go ss.monitorCollectorHeartbeats()
	go ss.doPublishTask()

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
	ss.kafkaClient.Close()
	ss.zkClient.Close()
	glog.Infof("ScheduleService stopped...")
}

func (ss *ScheduleService) monitorLeaderChanges() {
	watchChan, err := ss.zkClient.WatchElectionParticipants()
	if err != nil {
		panic("Failed to monitor leader changes")
	}

	for atomic.LoadInt32(&ss.started) != 0 {
		select{
		case <-watchChan:
			// register the watch immediately
			watch, err := ss.zkClient.WatchElectionParticipants()
			if err == nil {
				watchChan = watch
			}
			glog.Infof("Detect leader participants change")
			isLeader, err := ss.zkClient.IsLeader(ss.nodeGUID)
			if err == nil {
				if ss.isLeader != isLeader {
					glog.Warningf("Change the role from leader=%v to leader=%v", ss.isLeader, isLeader)
				}
				ss.isLeader = isLeader
			}
		}
	}
}

func (ss *ScheduleService) createTaskPublishJob(config base.BaseConfig) base.Job {
	interval, err := strconv.ParseInt(config[base.Interval], 10, 64)
	if err != nil {
		glog.Errorf("Failed to convert %s to integer, error=%s", config[base.Interval], err)
		return nil
	}

	interval = interval * int64(time.Second)
	job := base.NewJob(ss.publishTaskToKafka, time.Now().UnixNano(), interval, config)
	return job
}

func (ss *ScheduleService) publishTaskToKafka(params base.JobParam) error {
	config := params.(base.BaseConfig)
	ss.taskChan <- config
	return nil
}

func (ss *ScheduleService) doPublishTask() {
	// FIXME base.Key
	brokerConfig := base.BaseConfig{
		base.KafkaBrokers: ss.config[base.KafkaBrokers],
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
			// Only leader should publish the tasks
			if !ss.isLeader {
				continue
			}

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
	ss.liveCollectorsMutex.Lock()
	for host, apps := range ss.liveCollectors {
	    if ss.config[base.Heartbeat] != "kafka" {
			if _, ok := apps[config[base.App]]; ok {
				availableHosts = append(availableHosts, host)
			} else {
				glog.Warningf("Host=%s, App=%s has lost the heartbeat", host, config[base.App])
			}
		} else {
			if heartbeat, ok := apps[config[base.App]]; ok {
				lasttime, _ := strconv.ParseInt(heartbeat[base.Timestamp], 10, 64)
				if time.Now().UnixNano()-lasttime < heartbeatThreadhold {
					availableHosts = append(availableHosts, host)
				} else {
					glog.Warningf("Host=%s, App=%s has lost the heartbeat", host, config[base.App])
				}
			}
		}
	}
	ss.liveCollectorsMutex.Unlock()

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
	topicPartitions, err := ss.kafkaClient.TopicPartitions(topic)
	if err != nil {
		panic(fmt.Sprintf("Failed to get partitions for topic=%s", topic))
	}

	for _, partition := range topicPartitions[topic] {
		config := base.BaseConfig{
			base.KafkaTopic:		topic,
			base.KafkaPartition:    fmt.Sprintf("%d", partition),
			base.UseOffsetNewest:   "1",
		}

		reader := kafkareader.NewKafkaDataReader(ss.kafkaClient, config, writer, checkpoint)
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

func (ss *ScheduleService) monitorCollectorHeartbeats() {
	if ss.config[base.Heartbeat] != "kafka" {
		ss.doMonitorThroughZooKeeper()
	} else {
		ss.doMonitor(base.TaskStats)
	}
}

func (ss *ScheduleService) doMonitorThroughZooKeeper() {
	ss.refreshRegisteredCollectors()
	collectorChanges, err := ss.zkClient.ChildrenW(base.HeartbeatRoot)
	if err != nil {
		panic("Failed to monitor the collectors")
	}

	ticker := time.Tick(60 * time.Second)
	lastFreshed := time.Now().UnixNano()
	for atomic.LoadInt32(&ss.started) != 0 {
		select {
		case <-collectorChanges:
			collectorChanges, err = ss.zkClient.ChildrenW(base.HeartbeatRoot)
			if err != nil {
				continue
			}
			glog.Infof("Detect collectors change")
			ss.refreshRegisteredCollectors()
			lastFreshed = time.Now().UnixNano()

		case <-ticker:
			if time.Now().UnixNano() - lastFreshed > int64(60 * time.Second) {
				ss.refreshRegisteredCollectors()
			    lastFreshed = time.Now().UnixNano()
			}
		}
	}
}

func (ss *ScheduleService) refreshRegisteredCollectors() {
	// First get all registered collectors
	// base.HeartbeatRoot/<host>!<app>
	newLivings := make(map[string]map[string]base.BaseConfig)
	collectorHosts, err := ss.zkClient.Children(base.HeartbeatRoot)
	if err == nil {
		for _, hostCollector := range collectorHosts {
			hostApp := strings.Split(hostCollector, "!")
			if len(hostApp) != 2 {
				glog.Errorf("Invalid host collector=%s, expect in <host>!<app> format", hostCollector)
				continue
			}
			if newLivings[hostApp[0]] == nil {
				newLivings[hostApp[0]] = make(map[string]base.BaseConfig)
			}
			newLivings[hostApp[0]][hostApp[1]] = nil
		}
	}

	ss.liveCollectorsMutex.Lock()
	ss.liveCollectors = newLivings
	ss.liveCollectorsMutex.Unlock()
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

		ss.liveCollectorsMutex.Lock()
		if ss.liveCollectors[heartBeat[base.Host]] == nil {
			ss.liveCollectors[heartBeat[base.Host]] = make(map[string]base.BaseConfig)
		}
		ss.liveCollectors[heartBeat[base.Host]][heartBeat[base.App]] = heartBeat
		ss.liveCollectorsMutex.Unlock()
	}
}

func (ss *ScheduleService) monitorTasks() {
	// <-time.After(time.Duration(heartbeatThreadhold))
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
