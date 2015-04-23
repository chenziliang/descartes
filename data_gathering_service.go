package main

import (
	"bytes"
	"encoding/base64"
	"github.com/chenziliang/descartes/base"
	"github.com/golang/glog"
	"github.com/chenziliang/descartes/sources/snow"
	"github.com/chenziliang/descartes/sinks/splunk"
	kafkareader "github.com/chenziliang/descartes/sources/kafka"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
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

type JobCreationFunc func(config base.BaseConfig) base.Job

type JobFactory struct {
	creationTbl map[string]JobCreationFunc
	clients map[string]*base.KafkaClient
}

func NewJobFactory() *JobFactory {
	td := &JobFactory{
		creationTbl: make(map[string]JobCreationFunc),
		clients: make(map[string]*base.KafkaClient),
	}
	td.RegisterJobCreationFunc("snow", td.newSnowJob)
	td.RegisterJobCreationFunc("kafka", td.newKafkaJob)
	return td
}

func (factory *JobFactory) CreateJob(app string, config base.BaseConfig) base.Job{
	if createFunc, ok := factory.creationTbl[app]; ok {
		return createFunc(config)
	} else {
		glog.Errorf("%s is not registed.", app)
		return nil
	}
}

func (factory *JobFactory) newSnowJob(config base.BaseConfig) base.Job{
    url := encodeURL(config[base.ServerURL])
	topic := "snow" + "_" + url
	client, brokerConfigs := factory.getKafkaClient(config, topic, "0", topic)

	ckTopic := strings.Join([]string{"snow", config["Endpoint"], url, "ckpt"}, "_")
	config[base.CheckpointTopic] = ckTopic
	config[base.CheckpointKey] = ckTopic
	// FIXME partition
	config[base.CheckpointPartition] = "0"

	writer := kafkawriter.NewKafkaDataWriter(brokerConfigs)
	checkpoint := base.NewKafkaCheckpointer(client)
	reader := snow.NewSnowDataReader(base.BaseConfig(config), writer, checkpoint)
	interval, err := strconv.ParseInt(config["Interval"], 10, 64)
	if err != nil{
		// FIXME
		glog.Errorf("Failed to convert %s to integer, error=%s", config["Interval"], err)
		return nil
	}

    interval = interval * int64(time.Second)
	job := &ReaderJob{
		BaseJob: base.NewJob(base.JobFunc(nil), time.Now().UnixNano(), interval, config),
		reader: reader,
	}
	job.ResetFunc(job.callback)
	return job
}

func (factory *JobFactory) getKafkaClient(config base.BaseConfig, topic, partition, key string) (*base.KafkaClient, []base.BaseConfig) {
	brokers := strings.Split(config[base.Brokers], ";")
	sort.Sort(sort.StringSlice(brokers))
	sortedBrokers := strings.Join(brokers, ";")

	var brokerConfigs []base.BaseConfig
	for _, broker := range brokers {
		brokerConfig := base.BaseConfig{
			base.ServerURL: broker,
			base.Topic: topic,
			base.Partition: partition,
			base.Key: key,
		}
		brokerConfigs = append(brokerConfigs, brokerConfig)
	}

	if _, ok := factory.clients[sortedBrokers]; !ok {
		factory.clients[sortedBrokers] = base.NewKafkaClient(brokerConfigs, "")
	}
	return factory.clients[sortedBrokers], brokerConfigs
}

func (factory *JobFactory) newKafkaJob(config base.BaseConfig) base.Job{
	ckTopic := strings.Join([]string{config[base.Topic], config[base.Partition], "ckpt"}, "_")
	config[base.CheckpointTopic] = ckTopic
	config[base.CheckpointKey] = ckTopic
	// FIXME partition
	config[base.CheckpointPartition] = "0"

	client, _ := factory.getKafkaClient(config, config[base.Topic], config[base.Partition], config[base.Topic])

	glog.Errorf("%s", config)
	writer := splunk.NewSplunkDataWriter([]base.BaseConfig{config})
	checkpoint := base.NewKafkaCheckpointer(client)
	reader := kafkareader.NewKafkaDataReader(client, config, writer, checkpoint)
	job := &ReaderJob{
		BaseJob: base.NewJob(base.JobFunc(nil), time.Now().UnixNano(), int64(time.Hour * 24 * 365), config),
		reader: reader,
	}

	job.ResetFunc(job.callback)
	return job
}


func (factory *JobFactory) RegisterJobCreationFunc(app string, newFunc JobCreationFunc) {
	factory.creationTbl[app] = newFunc
}

type DataGatheringService struct {
	jobFactory *JobFactory
	jobScheduler  *base.Scheduler
	doneChan   chan bool
	started    int32
}


func NewDataGatheringService() *DataGatheringService {
	return &DataGatheringService{
		jobFactory: NewJobFactory(),
		jobScheduler: base.NewScheduler(),
		doneChan: make(chan bool, 3),
		started: 0,
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
	glog.Infof("DataGatheringService stopped...")
}

func (dgs *DataGatheringService) AddJob(app string, config base.BaseConfig) base.Job{
	job := dgs.jobFactory.CreateJob(app, config)
	dgs.jobScheduler.AddJobs([]base.Job{job})
	return job
}

func (dgs *DataGatheringService) RemoveJob(job base.Job) {
	dgs.jobScheduler.RemoveJobs([]base.Job{job})
}

func (dgs *DataGatheringService) UpdateJob(job base.Job) {
	dgs.jobScheduler.UpdateJobs([]base.Job{job})
}
