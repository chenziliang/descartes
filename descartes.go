package main

import (
	"encoding/json"
	"flag"
	"github.com/chenziliang/descartes/base"
	"github.com/chenziliang/descartes/services"
	"github.com/golang/glog"
	"io/ioutil"
	"time"
)

func getTasks(fileName string) (services.ConfigContainer, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		glog.Errorf("Failed to read %s, error=%s", fileName, err)
		return nil, err
	}

	tasks := make(services.ConfigContainer)
	err = json.Unmarshal(content, &tasks)
	if err != nil {
		glog.Errorf("Failed to unmarshal %s, error=%s", fileName, err)
		return nil, err
	}
	return tasks, nil
}

func main() {
	flag.Parse()

	snowTasks, err := getTasks("snow_tasks.json")
	if err != nil {
		return
	}

	kafkaTasks, err := getTasks("kafka_tasks.json")
	if err != nil {
		return
	}

	// glog.Errorf("snowTasks=%s", snowTasks["snow"][0])
	// glog.Errorf("kafkaTasks=%s", kafkaTasks)

	brokerConfig := base.BaseConfig{
		"ServerURL": "54.169.104.98:9092;52.74.147.99:9092;52.74.36.65:9092",
	}

	// Snow job
	snowDataGathering := services.NewDataGatheringService(brokerConfig)
	for k, tasks := range snowTasks {
		for _, task := range tasks {
			snowDataGathering.AddJob(k, task)
		}
	}
	snowDataGathering.Start()

	// Kafka job
	kafkaDataGathering := services.NewDataGatheringService(brokerConfig)
	topicMonitor := services.NewKafkaMetaDataMonitor(kafkaTasks, kafkaDataGathering)
	kafkaDataGathering.Start()
	topicMonitor.Start()

	time.Sleep(2 * time.Minute)

	snowDataGathering.Stop()
	kafkaDataGathering.Stop()
	topicMonitor.Stop()
	time.Sleep(1 * time.Second)
}
