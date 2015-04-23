package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/golang/glog"
	"github.com/chenziliang/descartes/base"
	"io/ioutil"
	"strings"
	"time"
)


/*var (
	snowURL = "https://ven01034.service-now.com"
	snowEndpoints = map[string]base.BaseConfig{
		"incident": base.BaseConfig{
			base.ServerURL: snowURL,
		    base.Username:         "admin",
		    base.Password:         "splunk123",
			"Endpoint": "incident",
			"TimestampField":       "sys_updated_on",
		    "NextRecordTime":       "2014-01-01+00:00:00",
		    "RecordCount":          "5",
			base.CheckpointDir:       ".",
			base.CheckpointNamespace: "snow_reader",
			base.CheckpointKey:       encodeURL(snowURL) + "_" + "incident",
			base.CheckpointTopic:     "snow",
			base.CheckpointPartition: "0",
		},
	}

	brokerConfigs = []base.BaseConfig{
		base.BaseConfig{
			base.ServerURL: "172.16.107.153:9092",
			base.Topic: "snow",
			base.Key: encodeURL(snowURL) + "_" + "incident",
		},
	}

	splunkConfigs = []base.BaseConfig{
		base.BaseConfig{
			base.ServerURL: "https://localhost:8089",
			base.Username:  "admin",
			base.Password:  "admin",
			base.Source:"descartes",
			base.Index:	"main",
		},
	}

	kafkaReaderConfig = map[string]string{
		base.ConsumerGroup:       "kafkaReader",
		base.Topic:               "snow",
		base.Partition:           "0",
	    base.CheckpointDir:       ".",
		base.CheckpointNamespace: "kafka_reader",
		base.CheckpointKey:       encodeURL(snowURL) + "_" + "incident",
		base.CheckpointTopic:     "SnowIncidentCkpt",
		base.CheckpointPartition: "0",
	}

	client = base.NewKafkaClient(brokerConfigs, "SnowClient")

	snowEventCheckpoint = base.NewKafkaCheckpointer(client)
	kafkaDataWriter = sinkkafka.NewKafkaDataWriter(brokerConfigs)
	snowDataReader = snow.NewSnowDataReader(snowEndpoints["incident"], kafkaDataWriter, snowEventCheckpoint)

	kafkaEventCheckpoint = base.NewKafkaCheckpointer(client)
	splunkDataWriter = splunk.NewSplunkDataWriter(splunkConfigs)
	kafkaDataReader = srckafka.NewKafkaDataReader(client, kafkaReaderConfig, splunkDataWriter, kafkaEventCheckpoint)
)

func indexDataToKafka() {
	snowDataReader.Start()
	go func() {
		for i := 0; i < 10; i++ {
			snowDataReader.IndexData()
			time.Sleep(time.Second * 10)
		}
	    snowDataReader.Stop()
	}()
}


func indexDataToSplunk() {
	kafkaDataReader.Start()
	go kafkaDataReader.IndexData()
}*/

func getTasks(fileName string) (map[string][]map[string]string, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		glog.Errorf("Failed to read %s, error=%s", fileName, err)
		return nil, err
	}

	tasks := make(map[string][]map[string]string)
	err = json.Unmarshal(content, &tasks)
	if err != nil {
		glog.Errorf("Failed to unmarshal %s, error=%s", fileName, err)
		return nil, err
	}
	return tasks, nil
}

func getTopicPartitions() (map[string][]int32, error) {
	brokerConfigs := []base.BaseConfig{
		base.BaseConfig{
			base.ServerURL: "172.16.107.153:9092",
		},
	}

	client := base.NewKafkaClient(brokerConfigs, "consumerClient")

	topicPartitions, err := client.TopicPartitions("")
	if err != nil {
		glog.Errorf("Failed to get topic and partitions, error=%s", err)
		return nil, err
	}
	return topicPartitions, nil
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


	snowDataGathering := NewDataGatheringService()
	for k, tasks := range snowTasks {
		for _, task := range tasks {
			snowDataGathering.AddJob(k, task)
		}
	}
	snowDataGathering.Start()

	topicPartitions, err := getTopicPartitions()
	if err != nil {
		return
	}
	glog.Errorf("snowTasks=%s", snowTasks["snow"][0])
	// glog.Errorf("kafkaTasks=%s", kafkaTasks)
	// glog.Errorf("topicPartitions=%s", topicPartitions)
	// return
	//time.Sleep(60 * time.Second)

	kafkaDataGathering := NewDataGatheringService()
	for app, tasks := range kafkaTasks {
		for _, task := range tasks {
			for topic, partitions := range topicPartitions {
				if strings.HasPrefix(topic, "snow_") && !strings.HasSuffix(topic, "_ckpt") {
					task[base.Topic] = topic
					task[base.Partition] = fmt.Sprintf("%d", partitions[0])
			        kafkaDataGathering.AddJob(app, task)
					glog.Infof("%s", topic)
				}
			}
		}
	}

    kafkaDataGathering.Start()
	time.Sleep(90 * time.Second)

	snowDataGathering.Stop()
	kafkaDataGathering.Stop()
	time.Sleep(1 * time.Second)
}
