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


func getGlobalConfig(fileName string) (base.BaseConfig, error) {
	content, err := ioutil.ReadFile(fileName)
	if err != nil {
		glog.Errorf("Failed to read %s, error=%s", fileName, err)
		return nil, err
	}

	configs := make(map[string]base.BaseConfig)
	err = json.Unmarshal(content, &configs)
	if err != nil {
		glog.Errorf("Failed to unmarshal %s, error=%s", fileName, err)
		return nil, err
	}

	globalConfig := make(map[string]string)
	for _, config := range configs {
		for k, v := range config {
			globalConfig[k] = v
		}
	}

	return globalConfig, nil
}


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

func handleDataCollection(globalConfig base.BaseConfig) {
	config := make(base.BaseConfig)
	for k, v := range globalConfig {
		config[k] = v
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

func writeTaskConfigs(globalConfig base.BaseConfig, snow_task_file, kafka_task_file string) {
	config := make(base.BaseConfig)
	for k, v := range globalConfig {
		config[k] = v
	}

	config[base.KafkaTopic] = base.TaskConfig
    config[base.Key] = base.TaskConfig

	configWriter := mgmt.NewTaskConfigWriter(config)
	configWriter.Start()
	defer configWriter.Stop()

	snowTasks, err := getTasks(snow_task_file)
	if err != nil {
		return
	}

	kafkaTasks, err := getTasks(kafka_task_file)
	if err != nil {
		return
	}

	var allTasks []base.BaseConfig
	for _, tasks := range snowTasks {
		for _, task := range tasks {
			task[base.KafkaTopic] = services.GenerateTopic(task[base.App],
			                        task[base.ServerURL], task[base.Username])
			for k, v := range globalConfig {
				task[k] = v
			}
			task[base.TaskConfigAction] = base.TaskConfigNew
			task[base.TaskConfigKey] = task[base.KafkaTopic] + "_" + task["Endpoint"]
			allTasks = append(allTasks, task)
		}
	}

	for _, tasks := range kafkaTasks {
		for _, task := range tasks {
			task[base.KafkaTopic] = services.GenerateTopic("snow",
			                         task["SourceServerURL"], task["SourceUsername"])
			for k, v := range globalConfig {
				task[k] = v
			}
			task[base.TaskConfigAction] = base.TaskConfigNew
			task[base.TaskConfigKey] = ""
			allTasks = append(allTasks, task)
		}
	}
	configWriter.Write(allTasks)
}

func handleScheduling(globalConfig base.BaseConfig) {
	config := make(base.BaseConfig)
	for k, v := range globalConfig {
		config[k] = v
	}

	schedule := services.NewScheduleService(config)
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
	role := flag.String("role", "", "[task_scheduler|data_collector|mgmt]")
	snow_task_file := flag.String("snow_task_file", "snow_tasks.json", "")
	kafka_task_file := flag.String("kafka_task_file", "kafka_tasks.json", "")
	flag.Parse()

	if *role == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	globalConfig, err := getGlobalConfig("global_settings.json")
	if err != nil {
		return
	}

	if *role == "task_scheduler" {
		handleScheduling(globalConfig)
	} else if *role == "data_collector" {
		handleDataCollection(globalConfig)
	} else if *role == "mgmt" {
	    writeTaskConfigs(globalConfig, *snow_task_file, *kafka_task_file)
	} else {
		flag.PrintDefaults()
		os.Exit(1)
	}
	time.Sleep(2 * time.Second)
}
