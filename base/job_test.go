package base

import (
	"fmt"
	"runtime"
	"sort"
	"strconv"
	"testing"
)

func TestJob(t *testing.T) {
	p := 4
	runtime.GOMAXPROCS(p)
	n := 1000
	jobs := buildJobs(n)
	for i := 0; i < n-1; i++ {
		jid, _ := strconv.Atoi(jobs[i].Id())
		if i+1 != jid {
			t.Errorf("Job Id has problem expect id=%d, got id=%d", i, jid)
		}
	}

	jobChan := make(chan []Job, 4)
	for i := 0; i < p; i++ {
		go func() {
			j := buildJobs(n)
			jobChan <- j
		}()
	}

	var ids = make(map[string]bool, n*4)

	for i := 0; i < p; i++ {
		jobs = <-jobChan
		fmt.Println(fmt.Sprintf("Get %d jobs", len(jobs)))
		for _, job := range jobs {
			if _, ok := ids[job.Id()]; ok {
				t.Errorf("Job ID=%d should not exist", job.Id())
			}
			ids[job.Id()] = true
		}
	}

	jobs = buildJobs(n * 1000)
	sort.Sort(JobList(jobs))

	for i := 0; i < n*1000-1; i++ {
		if !jobs[i].Less(jobs[i+1]) {
			t.Errorf("Job ID=%s doesn't less than Job ID=%s", jobs[i].Id(), jobs[i+1].Id())
		}
	}
}
