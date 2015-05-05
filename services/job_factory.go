package services

import (
	"bytes"
	"encoding/base64"
	"github.com/chenziliang/descartes/base"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
	"github.com/chenziliang/descartes/sinks/splunk"
	kafkareader "github.com/chenziliang/descartes/sources/kafka"
	"github.com/chenziliang/descartes/sources/snow"
	"github.com/golang/glog"
	"sort"
	"strconv"
	"strings"
	"time"
)

func encodeURL(url string) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer encoder.Close()
	encoder.Write([]byte(url))
	return buf.String()
}

func GenerateTopic(app, serverURL, username string) string {
	url := encodeURL(serverURL)
	return strings.Join([]string{app, url, username}, "_")
}

func createCheckpointer(config base.BaseConfig) base.Checkpointer {
	switch config[base.CheckpointMethod] {
	case "zookeeper":
		return base.NewZooKeeperCheckpointer(config)
	case "cassandra":
		return base.NewCassandraCheckpointer(config)
	case "kafka":
		client := base.NewKafkaClient(config, "")
		if client == nil {
			return nil
		}
		return base.NewKafkaCheckpointer(client)
	case "localfile":
		return base.NewFileCheckpointer()
	}
	return base.NewZooKeeperCheckpointer(config)
}

type ReaderJob struct {
	*base.BaseJob
	reader base.DataReader
}

func (job *ReaderJob) call(params base.JobParam) error {
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
	td.RegisterJobCreationHandler(base.KafkaApp, td.newKafkaJob)
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

func (factory *JobFactory) Apps() []string {
	var apps []string
	for app, _ := range factory.creationTbl {
		apps = append(apps, app)
	}
	return apps
}

func (factory *JobFactory) getKafkaClient(config base.BaseConfig) *base.KafkaClient {
	brokers := strings.Split(config[base.KafkaBrokers], ";")
	sort.Sort(sort.StringSlice(brokers))
	sortedBrokers := strings.Join(brokers, ";")

	if _, ok := factory.clients[sortedBrokers]; !ok {
		client := base.NewKafkaClient(config, "")
		if client == nil {
			return nil
		}
		factory.clients[sortedBrokers] = client
	} else {
		glog.Infof("Found cached KafkaClient for brokers=%s", sortedBrokers)
	}
	return factory.clients[sortedBrokers]
}

func (factory *JobFactory) CloseClients() {
	for _, client := range factory.clients {
		client.Close()
	}
}

func (factory *JobFactory) newSnowJob(config base.BaseConfig) base.Job {
	config[base.Key] = config[base.KafkaTopic]
	writer := kafkawriter.NewKafkaDataWriter(config)
	if writer == nil {
		return nil
	}

	keyParts := []string{"", encodeURL(config[base.ServerURL]), config[base.Username], config["Endpoint"]}
	config[base.Key] = strings.Join(keyParts, "/")
	checkpoint := createCheckpointer(config)
	if checkpoint == nil {
		return nil
	}

	reader := snow.NewSnowDataReader(config, writer, checkpoint)
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
		BaseJob: base.NewJob(nil, time.Now().UnixNano(), interval, config),
		reader:  reader,
	}
	job.ResetFunc(job.call)
	return job
}

func (factory *JobFactory) newKafkaJob(config base.BaseConfig) base.Job {
	client := factory.getKafkaClient(config)
	if client == nil {
		return nil
	}

	writer := splunk.NewSplunkDataWriter(config)
	if writer == nil {
		return nil
	}

	keyParts := []string{"", config[base.KafkaTopic], config[base.KafkaPartition]}
	config[base.Key] = strings.Join(keyParts, "/")
	checkpoint := createCheckpointer(config)
	if checkpoint == nil {
		return nil
	}

	reader := kafkareader.NewKafkaDataReader(client, config, writer, checkpoint)
	if reader == nil {
		return nil
	}

	job := &ReaderJob{
		BaseJob: base.NewJob(nil, time.Now().UnixNano(), int64(time.Hour*24*365), config),
		reader:  reader,
	}

	job.ResetFunc(job.call)
	return job
}

func (factory *JobFactory) RegisterJobCreationHandler(app string, newFunc JobCreationHandler) {
	factory.creationTbl[app] = newFunc
}
