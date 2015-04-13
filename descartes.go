package main

import (
	"flag"
	_ "github.com/chenziliang/descartes/base"
	"time"
)

func main() {
	flag.Parse()

	time.Sleep(time.Second)
}
