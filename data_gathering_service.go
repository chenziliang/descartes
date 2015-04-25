package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/chenziliang/descartes/base"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
	"github.com/chenziliang/descartes/sinks/splunk"
	kafkareader "github.com/chenziliang/descartes/sources/kafka"
	"github.com/chenziliang/descartes/sources/snow"
	"github.com/golang/glog"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

func encodeURL(url string) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer encoder.Close()
	encoder.Write([]byte(url))
	return buf.String()
}

func snowTopic(serverURL, username string) string {
	url := encodeURL(serverURL)
	return strings.Join([]string{"snow", url, username}, "_")
}

func snowCheckpointTopic(config base.BaseConfig) string {
	url := encodeURL(config[base.ServerURL])
    return strings.Join([]string{"snow", config["Endpoint"], url, "ckpt"}, "_")
}

func kafkaCheckpointTopic(config base.BaseConfig) string {
	return strings.Join([]string{config[base.Topic], config[base.Partition], "ckpt"}, "_")
}

type ReaderJob struct {
	*base.BaseJob
	reader base.DataReader
}

func (job *ReaderJob) callback() error {
	go job.reader.IndexData()
	return nil
}

func (job *ReaderJob) Start() {
	job.reader.Start()
}

func (job *ReaderJob) Stop() {
	job.reader.Stop()
}

type JobCreationHandler func(config base.BaseConfig) base.Job

type JobFactory struct {
	creationTbl map[string]JobCreationHandler
	clients     map[string]*base.KafkaClient
}

func NewJobFactory() *JobFactory {
	td := &JobFactory{
		creationTbl: make(map[string]JobCreationHandler),
		clients:     make(map[string]*base.KafkaClient),
	}
	td.RegisterJobCreationHandler("snow", td.newSnowJob)
	td.RegisterJobCreationHandler("kafka", td.newKafkaJob)
	return td
}

func (factory *JobFactory) CreateJob(app string, config base.BaseConfig) base.Job {
	if createFunc, ok := factory.creationTbl[app]; ok {
		return createFunc(config)
	} else {
		glog.Errorf("%s is not registed.", app)
		return nil
	}
}

func (factory *JobFactory) getKafkaClient(config base.BaseConfig, topic, key string) (*base.KafkaClient, []base.BaseConfig) {
	brokers := strings.Split(config[base.Brokers], ";")
	sort.Sort(sort.StringSlice(brokers))
	sortedBrokers := strings.Join(brokers, ";")

	var brokerConfigs []base.BaseConfig
	for _, broker := range brokers {
		brokerConfig := base.BaseConfig{
			base.ServerURL: broker,
			base.Topic:     topic,
			base.Key:       key,
		}
		brokerConfigs = append(brokerConfigs, brokerConfig)
	}

	if _, ok := factory.clients[sortedBrokers]; !ok {
		client := base.NewKafkaClient(brokerConfigs, "")
		if client == nil {
			return nil, nil
		}
		factory.clients[sortedBrokers] = client
	} else {
		glog.Infof("Found cached KafkaClient for brokers=%s", sortedBrokers)
	}
	return factory.clients[sortedBrokers], brokerConfigs
}

func (factory *JobFactory) CloseClients() {
	for _, client := range factory.clients {
		client.Close()
	}
}

func (factory *JobFactory) newSnowJob(config base.BaseConfig) base.Job {
	topic := snowTopic(config[base.ServerURL], config[base.Username])

	// FIXME partition
	client, brokerConfigs := factory.getKafkaClient(config, topic, topic)
	if client == nil {
		return nil
	}

	ckTopic := snowCheckpointTopic(config)
	config[base.CheckpointTopic] = ckTopic
	config[base.CheckpointKey] = ckTopic
	config[base.CheckpointPartition] = "0"

	writer := kafkawriter.NewKafkaDataWriter(brokerConfigs)
	if writer == nil {
		return nil
	}

	checkpoint := base.NewKafkaCheckpointer(client)
	if checkpoint == nil {
		return nil
	}

	reader := snow.NewSnowDataReader(base.BaseConfig(config), writer, checkpoint)
	if reader == nil {
		return nil
	}

	interval, err := strconv.ParseInt(config["Interval"], 10, 64)
	if err != nil {
		glog.Errorf("Failed to convert %s to integer, error=%s", config["Interval"], err)
		return nil
	}

	interval = interval * int64(time.Second)
	job := &ReaderJob{
		BaseJob: base.NewJob(base.JobFunc(nil), time.Now().UnixNano(), interval, config),
		reader:  reader,
	}
	job.ResetFunc(job.callback)
	return job
}

func (factory *JobFactory) newKafkaJob(config base.BaseConfig) base.Job {
	ckTopic := kafkaCheckpointTopic(config)
	config[base.CheckpointTopic] = ckTopic
	config[base.CheckpointKey] = ckTopic
	config[base.CheckpointPartition] = "0"

	client, _ := factory.getKafkaClient(config, config[base.Topic], config[base.Topic])

	writer := splunk.NewSplunkDataWriter([]base.BaseConfig{config})
	if writer == nil {
		return nil
	}

	checkpoint := base.NewKafkaCheckpointer(client)
	if checkpoint == nil {
		return nil
	}

	reader := kafkareader.NewKafkaDataReader(client, config, writer, checkpoint)
	if reader == nil {
		return nil
	}

	job := &ReaderJob{
		BaseJob: base.NewJob(base.JobFunc(nil), time.Now().UnixNano(), int64(time.Hour*24*365), config),
		reader:  reader,
	}

	job.ResetFunc(job.callback)
	return job
}

func (factory *JobFactory) RegisterJobCreationHandler(app string, newFunc JobCreationHandler) {
	factory.creationTbl[app] = newFunc
}

type DataGatheringService struct {
	jobFactory   *JobFactory
	jobScheduler *base.Scheduler
	started      int32
}

func NewDataGatheringService() *DataGatheringService {
	return &DataGatheringService{
		jobFactory:   NewJobFactory(),
		jobScheduler: base.NewScheduler(),
		started:      0,
	}
}

func (dgs *DataGatheringService) Start() {
	if !atomic.CompareAndSwapInt32(&dgs.started, 0, 1) {
		glog.Infof("DataGatheringService already started.")
		return
	}

	dgs.jobScheduler.Start()
	glog.Infof("DataGatheringService started...")
}

func (dgs *DataGatheringService) Stop() {
	if !atomic.CompareAndSwapInt32(&dgs.started, 1, 0) {
		glog.Infof("DataGatheringService already stopped.")
		return
	}

	dgs.jobScheduler.Stop()
	dgs.jobFactory.CloseClients()
	glog.Infof("DataGatheringService stopped...")
}

func (dgs *DataGatheringService) AddJob(app string, config base.BaseConfig) base.Job {
	job := dgs.jobFactory.CreateJob(app, config)
	if job != nil {
		dgs.jobScheduler.AddJobs([]base.Job{job})
	} else {
		glog.Errorf("Failed to create job for app=%s, config=%+v", app, config)
	}
	return job
}

func (dgs *DataGatheringService) RemoveJob(job base.Job) {
	dgs.jobScheduler.RemoveJobs([]base.Job{job})
}

func (dgs *DataGatheringService) UpdateJob(job base.Job) {
	dgs.jobScheduler.UpdateJobs([]base.Job{job})
}

// -------------- topic/partition handlers ----------
type ConfigContainer map[string][]base.BaseConfig

type KafkaTopicHandlerFactory struct {
	topicConfigs ConfigContainer
}

func NewKafkaTopicHandlerFactory(topicConfigs ConfigContainer) *KafkaTopicHandlerFactory {
	return &KafkaTopicHandlerFactory{
		topicConfigs: topicConfigs,
	}
}

func (factory *KafkaTopicHandlerFactory) handleSnowTopic(topic string, partition int32, dgs *DataGatheringService) bool {
	configs, ok := factory.topicConfigs["snow"]
	if !ok {
		glog.Infof("Not snow Kafka Topic handler is registered.")
		return false
	}

	if !strings.HasPrefix(topic, "snow_") || strings.HasSuffix(topic, "_ckpt") {
		return true
	}

	for _, config := range configs {
	    registeredTopic := snowTopic(config["SourceServerURL"], config["SourceUsername"])
		if topic == registeredTopic {
			task := make(map[string]string, len(config))
			for k, v := range config {
				task[k] = v
			}

			task[base.Topic] = topic
			task[base.Partition] = fmt.Sprintf("%d", partition)
			dgs.AddJob("kafka", task)
			glog.Infof("Handle new topic=%s, partition=%d for snow", topic, partition)
			return true
		}
	}
	glog.Errorf("No matched registed Kafka Topic handler found for topic=%s", topic)
	return false
}

// TopicPartitionHandler returns true if it handles the topic/partition, otherwise false
type TopicPartitionHandler func(topic string, partition int32, dgs *DataGatheringService) bool

type KafkaMetaDataMonitor struct {
	topicHandlerFactory *KafkaTopicHandlerFactory
	dgs                 *DataGatheringService
	client              *base.KafkaClient
	topicPartitions     map[string]map[int32]bool
	handlers            []TopicPartitionHandler
	started             int32
}

// TODO
// For now, only support one kafka cluster
// topicConfigs should contain brokers information
func NewKafkaMetaDataMonitor(topicConfigs ConfigContainer, dgs *DataGatheringService) *KafkaMetaDataMonitor {
	if topicConfigs == nil {
		glog.Errorf("nil topic configs")
		return nil
	}

	var brokers string
	for _, v := range topicConfigs {
		brokers = v[0][base.Brokers]
		break
	}

	if brokers == "" {
		glog.Errorf("No brokers specified in configs=%+v", topicConfigs)
		return nil
	}

	brokerConfigs := []base.BaseConfig{}

	for _, broker := range strings.Split(brokers, ";") {
		config := base.BaseConfig{
			base.ServerURL: broker,
		}

		brokerConfigs = append(brokerConfigs, config)
	}

	client := base.NewKafkaClient(brokerConfigs, "MonitorClient")

	mon := &KafkaMetaDataMonitor{
		topicHandlerFactory: NewKafkaTopicHandlerFactory(topicConfigs),
		dgs:                 dgs,
		client:              client,
		topicPartitions:     make(map[string]map[int32]bool, 10),
		handlers:            make([]TopicPartitionHandler, 0, 10),
	}
	mon.RegisterTopicHandler(mon.topicHandlerFactory.handleSnowTopic)
	return mon
}

func (mon *KafkaMetaDataMonitor) RegisterTopicHandler(handler TopicPartitionHandler) {
	mon.handlers = append(mon.handlers, handler)
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
	checkInterval := 10 * time.Second

	for atomic.LoadInt32(&mon.started) != 0 {
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
				if !strings.HasSuffix(topic, "_ckpt") {
					glog.Infof("Found new topic=%s, partition=%d", topic, id)
				}

				for _, handle := range mon.handlers {
					res := handle(topic, id, mon.dgs)
					if res {
						mon.topicPartitions[topic][id] = true
					}
				}
			}
		}
	}
}
