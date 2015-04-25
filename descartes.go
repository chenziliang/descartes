package main

import (
	"encoding/json"
	"flag"
	"github.com/golang/glog"
	"io/ioutil"
	"time"
)

func getTasks(fileName string) (ConfigContainer, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		glog.Errorf("Failed to read %s, error=%s", fileName, err)
		return nil, err
	}

	tasks := make(ConfigContainer)
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

	// Kafka job
	kafkaDataGathering := NewDataGatheringService()
	topicMonitor := NewKafkaMetaDataMonitor(kafkaTasks, kafkaDataGathering)
	kafkaDataGathering.Start()
	topicMonitor.Start()

	// Snow job
	snowDataGathering := NewDataGatheringService()
	for k, tasks := range snowTasks {
		for _, task := range tasks {
			snowDataGathering.AddJob(k, task)
		}
	}
	snowDataGathering.Start()

	time.Sleep(1 * time.Minute)

	snowDataGathering.Stop()
	kafkaDataGathering.Stop()
	topicMonitor.Stop()
	time.Sleep(1 * time.Second)
}
