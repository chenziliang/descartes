package main

import (
	"flag"
	"time"
	"github.com/chenziliang/descartes/plugins/snow"
	db "github.com/chenziliang/descartes/base"
)


func main() {
	flag.Parse()
	config := db.DataLoaderConfig {
		ServerURL: "https://ven01034.service-now.com",
		Username: "admin",
		Password: "splunk123",
	}

	writer := db.NewSplunkEventWriter(nil)
	writer.Start()
	dataLoader := snow.NewSnowDataLoader(
		config, writer, nil, "incident", "sys_updated_on", "2014-03-23+08:19:04")
	dataLoader.IndexData()
	writer.Stop()
	time.Sleep(3 * time.Second)
}
