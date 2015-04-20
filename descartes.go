package main

import (
	"bytes"
	"encoding/base64"
	"flag"
	"github.com/chenziliang/descartes/base"
	_ "github.com/golang/glog"
	"github.com/chenziliang/descartes/sources/snow"
	sinkkafka "github.com/chenziliang/descartes/sinks/kafka"
	srckafka "github.com/chenziliang/descartes/sources/kafka"
	"github.com/chenziliang/descartes/sinks/splunk"
	"time"
)

func encodeURL(url string) string {
	var buf bytes.Buffer
	encoder := base64.NewEncoder(base64.StdEncoding, &buf)
	defer encoder.Close()
	encoder.Write([]byte(url))
	return buf.String()
}


var (
	snowURL = "https://ven01034.service-now.com"
	snowEndpoints = map[string]*base.BaseConfig{
		"incident": &base.BaseConfig{
			ServerURL: snowURL,
		    Username:         "admin",
		    Password:         "splunk123",
			AdditionalConfig: map[string]string{
				"endpoint": "incident",
				"timestampField":       "sys_updated_on",
			    "nextRecordTime":       "2014-01-01+00:00:00",
			    "recordCount":          "5",
				base.CheckpointDir:       ".",
				base.CheckpointNamespace: "snow_reader",
				base.CheckpointKey:       encodeURL(snowURL) + "_" + "incident",
				base.CheckpointTopic:     "snow",
				base.CheckpointPartition: "0",
			},
		},
	}

	brokerConfigs = []*base.BaseConfig{
		&base.BaseConfig{
			ServerURL: "172.16.107.153:9092",
			AdditionalConfig: map[string]string{
				base.Topic: "snow",
				base.Key: encodeURL(snowURL) + "_" + "incident",
			},
		},
	}

	splunkConfigs = []*base.BaseConfig{
		&base.BaseConfig{
			ServerURL: "https://localhost:8089",
			Username:  "admin",
			Password:  "admin",
			AdditionalConfig: map[string]string{
				base.Source:"descartes",
				base.Index:	"main",
			},
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
}

func main() {
	flag.Parse()

	indexDataToKafka()
	indexDataToSplunk()

	time.Sleep(time.Second * 120)

	kafkaDataReader.Stop()

	time.Sleep(time.Second * 5)
}
