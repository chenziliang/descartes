package main

import (
	"flag"
	"time"
	"math/rand"
	"fmt"
	"github.com/chenziliang/descartes/sources/snow"
	"github.com/petar/GoLLRB/llrb"
	db "github.com/chenziliang/descartes/base"
)


func test(n int, id int) db.Func {
	return func() error {
	    fmt.Println(fmt.Sprintf("id=%d call every %d second.", id, n))
	    return nil
	}
}

func buildLLRBTree(jobs []*db.Job) *llrb.LLRB {
	now := time.Now().Unix()

	tree := llrb.New()
	for _, job := range jobs {
	    tree.InsertNoReplace(job)
	}
	fmt.Printf("Build llrb tree: %d sec\n", time.Now().Unix() - now)
	return tree
}

func buildJobs(n int) []*db.Job {
	now := time.Now().UnixNano()

	src := rand.NewSource(now)
	r := rand.New(src)

	s := int64(time.Second)
	jobs := make([]*db.Job, 0, n)
	for i := 0; i < n; i++ {
		job := db.NewJob(test(i + 1, i), 0, int64(s * (int64(r.Int()) % 3600 + 1)),
		                 make(map[string]string))
		jobs = append(jobs, job)
	}
	fmt.Printf("Build %d jobs tree: %d sec\n", n, time.Now().UnixNano() - now)
	return jobs
}

func buildJobsNoRand(n int) []*db.Job {
	now := time.Now().Unix()

	s := int64(time.Second)
	jobs := make([]*db.Job, 0, n)
	for i := 0; i < n; i++ {
		interval := i % 3600 + 1
		job := db.NewJob(test(interval, i + 1), 0,
		                 int64(interval) * s, make(map[string]string))
		jobs = append(jobs, job)
	}
	fmt.Printf("Build %d jobs tree: %d sec\n", n, time.Now().Unix() - now)
	return jobs
}

func testLLRB() {
	n := 1000000
	index := n / 2
	jobs := buildJobs(n)
	tree := buildLLRBTree(jobs)
	if tree.Len() != n {
		panic("Something is wrong, the number is not right")
	}

	now := time.Now().Unix()
	lastMin := tree.DeleteMin()
	for tree.Len() != 0 {
		nowMin := tree.DeleteMin()
		if !lastMin.Less(nowMin) {
			panic("Something is wrong with the llrb")
		}
	}
	fmt.Printf("Delete all mins %d sec\n", time.Now().Unix() - now)

	tree = buildLLRBTree(jobs)
	now = time.Now().Unix()
	lastMax := tree.DeleteMax()
	for tree.Len() != 0 {
		nowMax := tree.DeleteMax()
		if lastMax.Less(nowMax) {
			panic("Something is wrong with the llrb")
		}
	}
	fmt.Printf("Delete all maxs %d sec\n", time.Now().Unix() - now)

	tree = buildLLRBTree(jobs)
	now = time.Now().UnixNano()
	_ = tree.Max()
	fmt.Printf("Look for max: %d nano sec\n", time.Now().UnixNano() - now)

	now = time.Now().UnixNano()
	_ = tree.Min()
	fmt.Printf("Look for min: %d nano sec\n", time.Now().UnixNano() - now)

	for i := 0; i < 10; i++ {
		now = time.Now().UnixNano()
		item := tree.Get(jobs[index + i])
		fmt.Printf("Look for rand: %d nano sec\n", time.Now().UnixNano() - now)
		if item.(*db.Job).Id() != jobs[index + i].Id() {
			panic("Something wrong has happended")
		}
	}
}

func testSnowDataLoader() {
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

func main() {
	flag.Parse()

	n := 1000000
	jobs := buildJobsNoRand(n)
	scheduler := db.NewScheduler()
	// scheduler.SetMaxStartDelay(10 * int(time.Second))
	// scheduler.SetMaxStartDelay(0)
	scheduler.Start()
	scheduler.AddJobs(jobs)
	time.Sleep(1 * time.Hour)
	scheduler.Teardown()
	time.Sleep(time.Second)
}
