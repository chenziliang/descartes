package base

import (
	"fmt"
	"github.com/petar/GoLLRB/llrb"
	"math/rand"
	"testing"
	"time"
)

func test(n int, id int) Func {
	return func() error {
		// fmt.Println(fmt.Sprintf("id=%d call every %d second.", id, n))
		return nil
	}
}

func buildLLRBTree(jobs []*Job) *llrb.LLRB {
	now := time.Now().Unix()

	tree := llrb.New()
	for _, job := range jobs {
		tree.InsertNoReplace(job)
	}
	fmt.Printf("Build llrb tree: %d sec\n", time.Now().Unix()-now)
	return tree
}

func buildJobs(n int) []*Job {
	now := time.Now().UnixNano()

	src := rand.NewSource(now)
	r := rand.New(src)

	s := int64(time.Second)
	jobs := make([]*Job, 0, n)
	for i := 0; i < n; i++ {
		job := NewJob(test(i+1, i), 0, int64(s*(int64(r.Int())%3600+1)),
			make(map[string]string))
		jobs = append(jobs, job)
	}
	fmt.Printf("Build %d jobs tree: %d sec\n", n, time.Now().UnixNano()-now)
	return jobs
}

func buildJobsNoRand(n int) []*Job {
	now := time.Now().Unix()

	s := int64(time.Second)
	jobs := make([]*Job, 0, n)
	for i := 0; i < n; i++ {
		interval := i%3600 + 1
		job := NewJob(test(interval, i+1), 0,
			int64(interval)*s, make(map[string]string))
		jobs = append(jobs, job)
	}
	fmt.Printf("Build %d jobs tree: %d sec\n", n, time.Now().Unix()-now)
	return jobs
}

func TestScheduler(t *testing.T) {
	//n := 1000000
	n := 10
	jobs := buildJobsNoRand(n)
	scheduler := NewScheduler()
	// scheduler.SetMaxStartDelay(10 * int(time.Second))
	// scheduler.SetMaxStartDelay(0)
	scheduler.Start()
	scheduler.AddJobs(jobs)
	//time.Sleep(1 * time.Hour)
	time.Sleep(10 * time.Second)
	scheduler.Stop()
	time.Sleep(1 * time.Second)
}
