package main

import (
	"encoding/json"
	"flag"
	"github.com/chenziliang/descartes/base"
	"github.com/chenziliang/descartes/services"
	"github.com/chenziliang/descartes/mgmt"
	"github.com/golang/glog"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func getTasks(fileName string) (map[string][]base.BaseConfig, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		glog.Errorf("Failed to read %s, error=%s", fileName, err)
		return nil, err
	}

	tasks := make(map[string][]base.BaseConfig)
	err = json.Unmarshal(content, &tasks)
	if err != nil {
		glog.Errorf("Failed to unmarshal %s, error=%s", fileName, err)
		return nil, err
	}
	return tasks, nil
}

func setupSignalHandler() <-chan os.Signal {
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT,
                  syscall.SIGTERM, syscall.SIGQUIT)
	return c
}

func handleDataCollection(brokerConfig base.BaseConfig) {
	config := base.BaseConfig{
		base.Brokers: brokerConfig[base.Brokers],
	}

	collect := services.NewCollectService(config)
	if collect == nil {
		panic("Failed to create collect service")
	}
	collect.Start()

	c := setupSignalHandler()
	<-c

	// tear down
	collect.Stop()
}

func writeTaskConfigs(brokerConfig base.BaseConfig) {
	config := base.BaseConfig{
		base.Brokers: brokerConfig[base.Brokers],
		base.Topic: base.TaskConfig,
		base.Key: base.TaskConfig,
	}
	configWriter := mgmt.NewTaskConfigWriter(config)
	configWriter.Start()
	defer configWriter.Stop()

	snowTasks, err := getTasks("snow_tasks.json2")
	if err != nil {
		return
	}

	kafkaTasks, err := getTasks("kafka_tasks.json2")
	if err != nil {
		return
	}

	var allTasks []base.BaseConfig
	for _, tasks := range snowTasks {
		for _, task := range tasks {
			task[base.Topic] = services.GenerateTopic(task[base.App],
			                        task[base.ServerURL], task[base.Username])
			allTasks = append(allTasks, task)
		}
	}

	for _, tasks := range kafkaTasks {
		for _, task := range tasks {
			task[base.Topic] = services.GenerateTopic("snow",
			                         task["SourceServerURL"], task["SourceUsername"])
			allTasks = append(allTasks, task)
		}
	}
	configWriter.Write(allTasks)
}


func handleScheduling(brokerConfig base.BaseConfig) {
	schedule := services.NewScheduleService(brokerConfig)
	if schedule == nil {
		panic("Failed to create schedule service")
	}

	schedule.Start()

	c := setupSignalHandler()
	<-c

	// tear down
	schedule.Stop()
}

func main() {
	role := flag.String("role", "", "[task_scheduler|data_collector]")
	flag.Parse()

	if *role == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	brokerConfig := base.BaseConfig{
		"Brokers": "172.16.107.153:9092;172.16.107.153:9093;172.16.107.153:9094",
	}

	if *role == "task_scheduler" {
		handleScheduling(brokerConfig)
	} else if *role == "data_collector" {
		handleDataCollection(brokerConfig)
	} else if *role == "generate_config" {
	    writeTaskConfigs(brokerConfig)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}
	time.Sleep(2 * time.Second)
}
