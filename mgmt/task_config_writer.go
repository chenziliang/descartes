package mgmt

import (
	"github.com/chenziliang/descartes/base"
	"encoding/json"
	kafkawriter "github.com/chenziliang/descartes/sinks/kafka"
)

type TaskConfigWriter struct {
	brokerConfig base.BaseConfig
	writer       base.DataWriter
}


func NewTaskConfigWriter(brokerConfig base.BaseConfig) *TaskConfigWriter {
	writer := kafkawriter.NewKafkaDataWriter(brokerConfig)
	if writer == nil {
		return nil
	}
	return &TaskConfigWriter{
		brokerConfig: brokerConfig,
		writer: writer,
	}
}

func (writer *TaskConfigWriter) Start() {
	writer.writer.Start()
}

func (writer *TaskConfigWriter) Stop() {
	writer.writer.Stop()
}

func (writer *TaskConfigWriter) Write(configs []base.BaseConfig) error {
	var rawData [][]byte
	for _, config := range configs {
		data, err := json.Marshal(config)
		if err != nil {
			return err
		}
		rawData = append(rawData, data)
	}

	data := base.NewData(nil, rawData)
	return writer.writer.WriteData(data)
}
